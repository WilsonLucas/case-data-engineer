# BI Runbook — Comece Aqui

> Guia rapido para Analista de BI consumir as tabelas Gold do pipeline. 5 queries prontas + mapping pergunta -> tabela + dicas de performance.
> Todas as queries foram validadas contra o schema atual via SQL Warehouse Serverless.

## 1. Onde estao os dados

| Catalog | Schema | Conteudo | Quando usar |
|---------|--------|----------|-------------|
| `workspace` | `gold` | Star schema com 6 dims + 1 SCD2 + 4 facts + 1 view consolidada | **SEMPRE** — camada certa para BI |
| `workspace` | `silver` | Dados tipados intermediarios | Apenas para investigacao tecnica de DQ |
| `workspace` | `bronze` | Dados brutos string-typed | Nunca direto — apenas via Silver |

Comece sempre por `workspace.gold.vw_kpi_business`. Esta view ja faz os joins entre fatos e dimensoes — voce nao precisa.

## 2. Mapping pergunta -> tabela

| Pergunta de negocio | Tabela / View principal | Junte com |
|---------------------|------------------------|-----------|
| Quanto faturamos? | `gold.vw_kpi_business` | (nada — view ja agrega) |
| Qual canal vende mais? | `gold.vw_kpi_business` | (campo `canal_id` ja na view) |
| Qual regiao tem maior atraso? | `gold.vw_kpi_business` | (campos `regiao_nome` + `flag_atrasado` ja na view) |
| Quem sao os clientes top 10? | `gold.vw_kpi_business` | (campos `nome_cliente`, `segmento`, `uf` ja na view) |
| Como os tickets de atendimento evoluiram? | `gold.fact_ocorrencia` | `gold.dim_data` (via `data_id`) |
| Cliente X estava em qual segmento em janeiro? | `gold.dim_cliente_history` | range join via `effective_date BETWEEN ...` (ver secao 5) |
| Quais produtos cancelam mais? | `gold.fact_pedido` + `gold.fact_item` | `gold.dim_produto` (via `product_code`) |
| Lead time medio por carrier? | `gold.fact_entrega` | `dim_data` (via `data_envio_id`) |

## 3. 5 queries prontas (validadas — copy/paste)

### 3.1 Receita liquida total + ticket medio (REQ-NF-002 reconciliacao)

```sql
SELECT
    ROUND(SUM(valor_liquido), 2) AS receita_liquida_brl,
    COUNT(DISTINCT order_id) AS qtd_pedidos,
    ROUND(SUM(valor_liquido) / COUNT(DISTINCT order_id), 2) AS ticket_medio
FROM workspace.gold.vw_kpi_business
WHERE status_canonico IN ('FATURADO', 'EM_SEPARACAO');
-- Esperado: receita_liquida_brl = 1707675.84
```

### 3.2 Top 5 regioes por faturamento

```sql
SELECT
    regiao_nome,
    ROUND(SUM(valor_liquido), 2) AS receita_brl,
    COUNT(DISTINCT order_id) AS qtd_pedidos
FROM workspace.gold.vw_kpi_business
WHERE status_canonico IN ('FATURADO', 'EM_SEPARACAO')
  AND regiao_nome IS NOT NULL
GROUP BY regiao_nome
ORDER BY receita_brl DESC
LIMIT 5;
```

### 3.3 Taxa de cancelamento por canal

```sql
SELECT
    canal_id,
    COUNT(*) AS total_pedidos,
    SUM(CASE WHEN status_canonico = 'CANCELADO' THEN 1 ELSE 0 END) AS cancelados,
    ROUND(100.0 * SUM(CASE WHEN status_canonico = 'CANCELADO' THEN 1 ELSE 0 END) / COUNT(*), 2) AS taxa_cancel_pct
FROM workspace.gold.vw_kpi_business
WHERE canal_id IS NOT NULL
GROUP BY canal_id
ORDER BY taxa_cancel_pct DESC;
```

### 3.4 Lead time medio e taxa de atraso por mes (excluindo fim de semana e feriado)

```sql
SELECT
    d.ano,
    d.mes,
    d.mes_nome,
    COUNT(*) AS qtd_entregas,
    ROUND(AVG(e.lead_time_dias), 1) AS lead_time_medio_dias,
    ROUND(100.0 * SUM(CASE WHEN NOT e.on_time_flag THEN 1 ELSE 0 END) / COUNT(*), 1) AS taxa_atraso_pct
FROM workspace.gold.fact_entrega e
JOIN workspace.gold.dim_data d ON e.data_envio_id = d.data_id
WHERE d.eh_dia_util = TRUE
GROUP BY d.ano, d.mes, d.mes_nome
ORDER BY d.ano, d.mes;
```

### 3.5 Top 10 clientes por volume + segmento + UF

```sql
SELECT
    customer_code,
    nome_cliente,
    segmento,
    uf,
    ROUND(SUM(valor_liquido), 2) AS gasto_total_brl,
    COUNT(DISTINCT order_id) AS qtd_pedidos
FROM workspace.gold.vw_kpi_business
WHERE status_canonico = 'FATURADO'
GROUP BY customer_code, nome_cliente, segmento, uf
ORDER BY gasto_total_brl DESC
LIMIT 10;
```

## 4. Dicas de performance

- **Use `vw_kpi_business`** sempre que precisar de pedido + cliente + canal + regiao + vendedor juntos. A view ja faz os joins corretos. Voce escreve `WHERE`/`GROUP BY` sem se preocupar com chaves.
- **Nomes reais na view** (validar antes de copiar de outros documentos): `valor_liquido` (nao `net_amount`), `data_pedido` (nao `data_id`), `segmento` (nao `cliente_segmento`), `uf` (nao `estado`), `canal_id` (nao `channel_code`), `flag_atrasado`/`flag_cancelado` (nao `eh_*`), `nome_cliente` (nao `nome`).
- **Filtre antes de agregar**: `WHERE status_canonico IN (...)` reduz o conjunto antes de `SUM`. Catalyst optimizer ja faz isso, mas explicitar ajuda na leitura.
- **Evite `SELECT *`** em fatos — sempre liste colunas. Reduz I/O em colunas string longas.
- **`fact_entrega` agrega por pedido pelo PIOR caso** (`MAX(atraso_dias)`, `MIN(shipped_at)`) quando consultada via `vw_kpi_business`. Para analise por remessa fisica, va direto na `fact_entrega`.
- **Time travel**: `SELECT * FROM workspace.gold.fact_pedido VERSION AS OF 0` para ver primeira versao gravada. Util para auditoria.

## 5. Quando precisar de historico (SCD2)

Para reconstruir o **perfil de um cliente em uma data passada** (ex: "em qual segmento o cliente X estava em janeiro?"), use `dim_cliente_history`. Note: `fact_pedido.data_id` eh INT no formato yyyymmdd — converter para DATE no join via `TO_DATE(CAST(... AS STRING), 'yyyyMMdd')`:

```sql
SELECT
    h.customer_code,
    h.segmento,    -- segmento NA DATA do pedido (nao o atual)
    h.uf,
    h.cidade,
    h.status_cliente,
    p.order_id,
    p.net_amount,
    h.effective_date,
    h.end_date,
    h.is_current
FROM workspace.gold.fact_pedido p
JOIN workspace.gold.dim_cliente_history h
    ON p.customer_code = h.customer_code
    AND TO_DATE(CAST(p.data_id AS STRING), 'yyyyMMdd')
        BETWEEN h.effective_date AND h.end_date
WHERE p.customer_code = 'CUST_001'  -- substituir pelo customer_code real
ORDER BY p.data_id;
```

Para analise corrente normal, **use `dim_cliente` SCD1** (mais simples e rapido):

```sql
SELECT customer_code, nome_cliente, segmento, uf, status_cliente
FROM workspace.gold.dim_cliente
WHERE customer_code = 'CUST_001';
```

## 6. Checklist do BI consumer

- [ ] Conectou na catalog `workspace`, schema `gold`
- [ ] Usou `vw_kpi_business` como ponto de entrada para pedidos
- [ ] Usou nomes corretos: `valor_liquido`, `segmento`, `uf`, `canal_id`, `flag_*`
- [ ] Aplicou filtro de `status_canonico` quando necessario (`FATURADO` exclui `CANCELADO`)
- [ ] Para excluir fim de semana/feriado: `JOIN dim_data ON ... WHERE eh_dia_util = TRUE`
- [ ] Validou que `SUM(valor_liquido) WHERE status_canonico IN ('FATURADO','EM_SEPARACAO')` = R$ 1.707.675,84 (gate REQ-NF-002)
- [ ] Para historico: usou `dim_cliente_history` com range join via `TO_DATE(CAST(data_id AS STRING), 'yyyyMMdd')`

## 7. Quando algo nao bate

- Verifique se filtro de status esta correto (a maioria dos KPIs exclui `CANCELADO`).
- Confirme nomes de colunas — `vw_kpi_business` tem schema diferente das fact tables (e.g. `valor_liquido` na view vs `net_amount` em `fact_pedido`).
- Para dim com SCD2, range join em vez de equi-join (e converter INT data_id para DATE).
- Reconciliacao deve sempre bater Bronze=Silver=Gold (ver `99_validation.py` Teste 2).
- Em duvida, abra issue no repo ou consulte ADRs em `docs/adr/`.
