# Perguntas de Negócio - Respostas via SQL

> Cada pergunta do PDF respondida com SQL pronto pra rodar sobre o modelo Gold.

---

## 1. Como o negócio performou no período analisado?

**Indicador:** evolução de receita líquida e quantidade de pedidos por mês.

```sql
SELECT
    ano_mes,
    COUNT(*) AS qtd_pedidos,
    SUM(valor_liquido) AS receita_liquida,
    AVG(valor_liquido) AS ticket_medio,
    SUM(CASE WHEN flag_cancelado THEN 1 ELSE 0 END) AS pedidos_cancelados,
    ROUND(100.0 * SUM(CASE WHEN flag_cancelado THEN 1 ELSE 0 END) / COUNT(*), 2) AS taxa_cancelamento_pct
FROM gold.vw_kpi_business
GROUP BY ano_mes
ORDER BY ano_mes;
```

**Visualização sugerida:** linha de tempo dupla (receita + qtd pedidos), com tabela complementar de taxas.

---

## 2. Quais regiões, canais e categorias apresentam melhor e pior desempenho?

### 2.1 Top 5 regiões por receita líquida

```sql
SELECT
    regiao_nome,
    COUNT(*) AS qtd_pedidos,
    SUM(valor_liquido) AS receita_liquida,
    AVG(valor_liquido) AS ticket_medio,
    ROUND(100.0 * SUM(CASE WHEN flag_atrasado THEN 1 ELSE 0 END) / COUNT(*), 2) AS taxa_atraso_pct
FROM gold.vw_kpi_business
WHERE NOT flag_cancelado
GROUP BY regiao_nome
ORDER BY receita_liquida DESC;
```

### 2.2 Performance por canal

```sql
SELECT
    canal_nome,
    COUNT(*) AS qtd_pedidos,
    SUM(valor_liquido) AS receita_liquida,
    AVG(valor_liquido) AS ticket_medio,
    ROUND(100.0 * SUM(CASE WHEN flag_cancelado THEN 1 ELSE 0 END) / COUNT(*), 2) AS taxa_cancelamento_pct,
    ROUND(100.0 * SUM(CASE WHEN flag_com_ocorrencia THEN 1 ELSE 0 END) / COUNT(*), 2) AS taxa_ocorrencia_pct
FROM gold.vw_kpi_business
GROUP BY canal_nome
ORDER BY receita_liquida DESC;
```

### 2.3 Top categorias de produto (granularidade item)

```sql
SELECT
    p.categoria,
    COUNT(*) AS qtd_itens_vendidos,
    SUM(i.quantity) AS volume,
    SUM(i.total_item) AS receita_categoria,
    AVG(i.unit_price) AS preco_medio_unitario
FROM gold.fact_item i
JOIN gold.dim_produto p ON i.product_code = p.product_code
JOIN gold.fact_pedido pe ON i.order_id = pe.order_id
WHERE pe.status_canonico = 'FATURADO'
GROUP BY p.categoria
ORDER BY receita_categoria DESC;
```

---

## 3. Onde estão os principais gargalos operacionais?

### 3.1 Taxa de atraso por região × canal

```sql
SELECT
    regiao_nome,
    canal_nome,
    COUNT(*) AS qtd_entregas,
    ROUND(100.0 * SUM(CASE WHEN flag_atrasado THEN 1 ELSE 0 END) / COUNT(*), 2) AS taxa_atraso_pct,
    AVG(atraso_dias) AS atraso_medio_dias,
    AVG(lead_time_dias) AS lead_time_medio_dias
FROM gold.vw_kpi_business
WHERE flag_atrasado IS NOT NULL
GROUP BY regiao_nome, canal_nome
HAVING COUNT(*) >= 5
ORDER BY taxa_atraso_pct DESC;
```

### 3.2 Volume de ocorrências por tipo

```sql
SELECT
    o.event_type,
    o.severity,
    COUNT(*) AS qtd_ocorrencias,
    SUM(o.severity_score) AS score_total,
    COUNT(DISTINCT o.order_id) AS pedidos_afetados
FROM gold.fact_ocorrencia o
GROUP BY o.event_type, o.severity
ORDER BY score_total DESC;
```

### 3.3 Pedidos com múltiplos problemas (cancelado + ocorrência + atraso)

```sql
SELECT
    order_id,
    canal_nome,
    regiao_nome,
    valor_liquido,
    flag_cancelado,
    flag_atrasado,
    flag_com_ocorrencia,
    qtd_ocorrencias
FROM gold.vw_kpi_business
WHERE flag_cancelado OR flag_atrasado OR flag_com_ocorrencia
  AND (CASE WHEN flag_cancelado THEN 1 ELSE 0 END +
       CASE WHEN flag_atrasado THEN 1 ELSE 0 END +
       CASE WHEN flag_com_ocorrencia THEN 1 ELSE 0 END) >= 2
ORDER BY valor_liquido DESC
LIMIT 50;
```

---

## 4. Existem sinais de perda de receita ou ineficiência?

### 4.1 Receita perdida por cancelamento

```sql
SELECT
    ano_mes,
    canal_nome,
    SUM(CASE WHEN flag_cancelado THEN valor_liquido ELSE 0 END) AS receita_cancelada,
    SUM(valor_liquido) AS receita_total,
    ROUND(100.0 * SUM(CASE WHEN flag_cancelado THEN valor_liquido ELSE 0 END) / NULLIF(SUM(valor_liquido), 0), 2) AS pct_perda
FROM gold.vw_kpi_business
GROUP BY ano_mes, canal_nome
HAVING SUM(CASE WHEN flag_cancelado THEN valor_liquido ELSE 0 END) > 0
ORDER BY ano_mes, pct_perda DESC;
```

### 4.2 Análise de descontos vs cancelamento (descontos altos correlacionam com cancelamento?)

```sql
WITH bins AS (
    SELECT
        order_id,
        CASE
            WHEN gross_amount = 0 THEN 'sem_desconto'
            WHEN discount_amount / gross_amount < 0.05 THEN '0-5%'
            WHEN discount_amount / gross_amount < 0.15 THEN '5-15%'
            WHEN discount_amount / gross_amount < 0.30 THEN '15-30%'
            ELSE 'acima_30%'
        END AS faixa_desconto,
        flag_cancelado
    FROM gold.vw_kpi_business
    WHERE gross_amount > 0
)
SELECT
    faixa_desconto,
    COUNT(*) AS qtd_pedidos,
    SUM(CASE WHEN flag_cancelado THEN 1 ELSE 0 END) AS cancelados,
    ROUND(100.0 * SUM(CASE WHEN flag_cancelado THEN 1 ELSE 0 END) / COUNT(*), 2) AS taxa_cancelamento_pct
FROM bins
GROUP BY faixa_desconto
ORDER BY faixa_desconto;
```

### 4.3 Eficiência por vendedor (receita / qtd pedidos)

```sql
SELECT
    v.seller_name,
    ca.nome AS canal,
    r.regiao_nome,
    COUNT(*) AS qtd_pedidos,
    SUM(p.net_amount) AS receita_total,
    AVG(p.net_amount) AS ticket_medio,
    SUM(CASE WHEN p.status_canonico = 'CANCELADO' THEN 1 ELSE 0 END) AS pedidos_cancelados
FROM gold.fact_pedido p
JOIN gold.dim_vendedor v ON p.seller_id = v.seller_id
LEFT JOIN gold.dim_canal ca ON v.canal_id = ca.canal_id
LEFT JOIN gold.dim_regiao r ON v.regional_code = r.regional_code
WHERE v.status = 'ATIVO'
GROUP BY v.seller_name, ca.nome, r.regiao_nome
HAVING COUNT(*) >= 3
ORDER BY receita_total DESC;
```

---

## 5. Quais ações práticas poderiam ser priorizadas pela liderança?

### 5.1 Heatmap canal × região com piores indicadores combinados

```sql
SELECT
    canal_nome,
    regiao_nome,
    COUNT(*) AS qtd_pedidos,
    SUM(valor_liquido) AS receita,
    -- Score combinado de problemas (0-300, quanto maior, pior)
    ROUND(
        100.0 * SUM(CASE WHEN flag_cancelado THEN 1 ELSE 0 END) / COUNT(*) +
        100.0 * SUM(CASE WHEN flag_atrasado THEN 1 ELSE 0 END) / COUNT(*) +
        100.0 * SUM(CASE WHEN flag_com_ocorrencia THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS score_problema_combinado
FROM gold.vw_kpi_business
GROUP BY canal_nome, regiao_nome
HAVING COUNT(*) >= 5
ORDER BY score_problema_combinado DESC, receita DESC
LIMIT 20;
```

### 5.2 Clientes com maior valor em risco (alto LTV + alta taxa de problemas)

```sql
WITH cliente_metrics AS (
    SELECT
        customer_code,
        cliente_nome,
        cliente_segmento,
        COUNT(*) AS qtd_pedidos,
        SUM(valor_liquido) AS valor_total,
        100.0 * SUM(CASE WHEN flag_cancelado OR flag_atrasado OR flag_com_ocorrencia THEN 1 ELSE 0 END) / COUNT(*) AS pct_problemas
    FROM gold.vw_kpi_business
    GROUP BY customer_code, cliente_nome, cliente_segmento
)
SELECT *
FROM cliente_metrics
WHERE qtd_pedidos >= 3
  AND pct_problemas > 30  -- mais de 30% dos pedidos com algum problema
ORDER BY valor_total DESC
LIMIT 30;
```

### 5.3 Produtos mais problemáticos (top de devolução/cancelamento)

```sql
SELECT
    pr.nome AS produto,
    pr.categoria,
    COUNT(DISTINCT i.order_id) AS pedidos_com_produto,
    SUM(i.quantity) AS volume_total,
    COUNT(DISTINCT CASE WHEN p.status_canonico = 'CANCELADO' THEN i.order_id END) AS pedidos_cancelados,
    COUNT(DISTINCT o.ticket_id) AS ocorrencias_relacionadas,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.status_canonico = 'CANCELADO' THEN i.order_id END) / NULLIF(COUNT(DISTINCT i.order_id), 0), 2) AS taxa_cancelamento_pct
FROM gold.fact_item i
JOIN gold.dim_produto pr ON i.product_code = pr.product_code
JOIN gold.fact_pedido p ON i.order_id = p.order_id
LEFT JOIN gold.fact_ocorrencia o ON i.order_id = o.order_id
GROUP BY pr.nome, pr.categoria
HAVING COUNT(DISTINCT i.order_id) >= 5
ORDER BY taxa_cancelamento_pct DESC, ocorrencias_relacionadas DESC
LIMIT 20;
```

---

## Síntese - recomendações sugeridas para liderança (preencher após executar queries)

> Esta seção é preenchida no `EXECUTIVE_SUMMARY.md` após executar as queries reais. Espaços para insights:

- **Maior oportunidade de receita:** `<resultado da query 1>` (ex: canal X em região Y vem crescendo Z%)
- **Maior gargalo operacional:** `<resultado da query 3.1>` (ex: entregas para região W com taxa de atraso de Z%)
- **Maior risco de churn:** `<resultado da query 5.2>` (ex: N clientes com LTV total R$ Y e mais de 30% de pedidos problemáticos)
- **Quick win recomendado:** `<derivado dos dados>` (ex: revisar política de desconto >30% que correlaciona com cancelamento de Y%)
