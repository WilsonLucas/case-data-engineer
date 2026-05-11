# BI Runbook — Comece Aqui

> Guia rapido para Analista de BI consumir as tabelas Gold do pipeline Levva. 5 queries prontas + mapping pergunta -> tabela + dicas de performance.

## 1. Onde estao os dados

| Catalog | Schema | Conteudo | Quando usar |
|---------|--------|----------|-------------|
| `workspace` | `gold` | Star schema com 6 dims + 4 facts + 1 view consolidada | **SEMPRE** — camada certa para BI |
| `workspace` | `silver` | Dados tipados intermediarios | Apenas para investigacao tecnica de DQ |
| `workspace` | `bronze` | Dados brutos string-typed | Nunca direto — apenas via Silver |

Comece sempre por `gold.vw_kpi_business`. Esta view ja faz os joins entre fatos e dimensoes — voce nao precisa.

## 2. Mapping pergunta -> tabela

| Pergunta de negocio | Tabela / View principal | Junte com |
|---------------------|------------------------|-----------|
| Quanto faturamos? | `gold.vw_kpi_business` | (nada — view ja agrega) |
| Qual canal vende mais? | `gold.fact_pedido` | `gold.dim_canal` (via `channel_code`) |
| Qual regiao tem maior atraso? | `gold.fact_entrega` | `gold.dim_vendedor` -> `gold.dim_regiao` |
| Quem sao os clientes top 10? | `gold.fact_pedido` | `gold.dim_cliente` (via `customer_code`) |
| Como os tickets de atendimento evoluiram? | `gold.fact_ocorrencia` | `gold.dim_data` (via `data_id`) |
| Cliente X estava em qual segmento em janeiro? | `gold.dim_cliente_history` | range join via `effective_date BETWEEN ...` |
| Quais produtos cancelam mais? | `gold.fact_pedido` + `gold.fact_item` | `gold.dim_produto` (via `product_code`) |
| Lead time medio por carrier? | `gold.fact_entrega` | `gold.dim_canal` se aplicavel |

## 3. 5 queries prontas

### 3.1 Receita liquida total no periodo (REQ-NF-002 reconciliacao)

```sql
SELECT
    ROUND(SUM(net_amount), 2) AS receita_liquida_brl,
    COUNT(DISTINCT order_id) AS qtd_pedidos,
    ROUND(SUM(net_amount) / COUNT(DISTINCT order_id), 2) AS ticket_medio
FROM workspace.gold.fact_pedido
WHERE status_canonico IN ('FATURADO', 'EM_SEPARACAO');
-- Esperado: receita_liquida_brl = 1707675.84
```

### 3.2 Top 5 regioes por faturamento

```sql
SELECT
    r.regiao_canonica,
    ROUND(SUM(p.net_amount), 2) AS receita_brl,
    COUNT(DISTINCT p.order_id) AS qtd_pedidos
FROM workspace.gold.fact_pedido p
JOIN workspace.gold.dim_vendedor v ON p.seller_id = v.seller_id
JOIN workspace.gold.dim_regiao r ON v.regional_code = r.regional_code
WHERE p.status_canonico IN ('FATURADO', 'EM_SEPARACAO')
GROUP BY r.regiao_canonica
ORDER BY receita_brl DESC
LIMIT 5;
```

### 3.3 Taxa de cancelamento por canal

```sql
SELECT
    c.channel_name,
    COUNT(*) AS total_pedidos,
    SUM(CASE WHEN p.status_canonico = 'CANCELADO' THEN 1 ELSE 0 END) AS cancelados,
    ROUND(100.0 * SUM(CASE WHEN p.status_canonico = 'CANCELADO' THEN 1 ELSE 0 END) / COUNT(*), 2) AS taxa_cancel_pct
FROM workspace.gold.fact_pedido p
LEFT JOIN workspace.gold.dim_canal c ON p.channel_code = c.channel_code
GROUP BY c.channel_name
ORDER BY taxa_cancel_pct DESC;
```

### 3.4 Taxa de atraso e lead time medio por mes (excluindo fim de semana e feriado)

```sql
SELECT
    d.ano,
    d.mes,
    COUNT(*) AS qtd_entregas,
    ROUND(AVG(e.lead_time_dias), 1) AS lead_time_medio_dias,
    ROUND(100.0 * SUM(CASE WHEN e.eh_atrasado THEN 1 ELSE 0 END) / COUNT(*), 1) AS taxa_atraso_pct
FROM workspace.gold.fact_entrega e
JOIN workspace.gold.dim_data d ON e.data_envio_id = d.data_id
WHERE d.eh_dia_util = TRUE
GROUP BY d.ano, d.mes
ORDER BY d.ano, d.mes;
```

### 3.5 Top 10 clientes por volume + segmento

```sql
SELECT
    c.customer_code,
    c.cliente_segmento,
    c.estado,
    ROUND(SUM(p.net_amount), 2) AS gasto_total_brl,
    COUNT(DISTINCT p.order_id) AS qtd_pedidos
FROM workspace.gold.fact_pedido p
JOIN workspace.gold.dim_cliente c ON p.customer_code = c.customer_code
WHERE p.status_canonico = 'FATURADO'
GROUP BY c.customer_code, c.cliente_segmento, c.estado
ORDER BY gasto_total_brl DESC
LIMIT 10;
```

## 4. Dicas de performance

- **Use `vw_kpi_business`** sempre que precisar de pedido + cliente + canal + regiao + vendedor juntos. A view ja faz os joins corretos. Voce escreve `WHERE`/`GROUP BY` sem se preocupar com chaves.
- **Filtre antes de agregar**: `WHERE status_canonico IN (...)` reduz o conjunto antes de `SUM`. Catalyst optimizer ja faz isso, mas explicitar ajuda.
- **Evite `SELECT *`** em fatos — sempre liste colunas. Reduz I/O em colunas string longas.
- **`fact_entrega` agrega por pedido pelo PIOR caso** (MAX(atraso_dias), MIN(shipped_at)) quando consultada via `vw_kpi_business`. Para analise por remessa fisica, va direto na `fact_entrega`.
- **Time travel**: `SELECT * FROM workspace.gold.fact_pedido VERSION AS OF 0` para ver primeira versao gravada. Util para auditar mudancas.

## 5. Quando precisar de historico (SCD2)

Para reconstruir o **perfil de um cliente em uma data passada** (ex: "em qual segmento o cliente X estava em janeiro?"), use `dim_cliente_history`:

```sql
SELECT
    c.customer_code,
    c.cliente_segmento,  -- segmento NA DATA do pedido (nao o atual)
    c.estado,
    p.order_id,
    p.order_date,
    p.net_amount
FROM workspace.gold.fact_pedido p
JOIN workspace.gold.dim_cliente_history c
    ON p.customer_code = c.customer_code
    AND CAST(p.data_id AS DATE) BETWEEN c.effective_date AND c.end_date
WHERE p.customer_code = 'CUST-XXX'
ORDER BY p.order_date;
```

Para analise corrente normal, **use `dim_cliente` SCD1**, mais simples e rapido.

## 6. Checklist do BI consumer

- [ ] Conectou na catalog `workspace`, schema `gold`
- [ ] Usou `vw_kpi_business` como ponto de entrada para pedidos
- [ ] Aplicou filtro de `status_canonico` quando necessario (FATURADO vs CANCELADO)
- [ ] Usou `dim_data.eh_dia_util` para excluir fim de semana e feriados
- [ ] Validou que `SUM(net_amount)` bate com R$ 1.707.675,84 quando filtrar `FATURADO + EM_SEPARACAO` (gate REQ-NF-002)
- [ ] Para historico: usou `dim_cliente_history` com range join

## 7. Quando algo nao bate

- Verifique se filtro de status esta correto (a maioria dos KPIs exclui CANCELADO).
- Confirme que joins usam as chaves naturais corretas (`customer_code`, `seller_id`, `product_code`, `data_id`, `channel_code`, `regional_code`).
- Para dim com SCD2, range join em vez de equi-join.
- Reconciliacao deve sempre bater Bronze=Silver=Gold (ver `99_validation.py` Teste 2).
- Em duvida, abra issue no repo ou consulte ADRs em `docs/adr/`.
