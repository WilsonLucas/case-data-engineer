# Databricks notebook source
# MAGIC %md
# MAGIC # View: workspace.case_levva_gold.vw_kpi_business
# MAGIC ## Objetivo:
# MAGIC View pré-joinada granular pedido que consolida `fact_pedido` com todas as dimensões e flags operacionais (`flag_cancelado`, `flag_atrasado`, `flag_com_ocorrencia`). O Analista de BI consulta direto sobre essa view sem precisar fazer joins manualmente — habilita dashboards de receita líquida, taxa de cancelamento, taxa de atraso, ticket médio e volume de ocorrências por qualquer combinação de dimensão.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `workspace.case_levva_gold.fact_pedido` | Driver (granularidade da view) |
# MAGIC | `workspace.case_levva_gold.fact_entrega` | Agregada por order_id para `flag_atrasado` e tempo médio |
# MAGIC | `workspace.case_levva_gold.fact_ocorrencia` | Agregada por order_id para `flag_com_ocorrencia` |
# MAGIC | `workspace.case_levva_gold.dim_*` | Cliente, produto, canal, região, vendedor, data |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC: schema `workspace.case_levva_gold` |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.case_levva_gold.vw_kpi_business AS
# MAGIC SELECT
# MAGIC -- Identificadores
# MAGIC p.order_id,
# MAGIC p.customer_code,
# MAGIC p.seller_id,
# MAGIC
# MAGIC -- Tempo
# MAGIC d.data_completa AS data_pedido,
# MAGIC d.ano,
# MAGIC d.ano_mes,
# MAGIC d.trimestre,
# MAGIC d.dia_semana,
# MAGIC d.fim_de_semana,
# MAGIC
# MAGIC -- Atributos de cliente (selecionar conforme schema real do workspace.case_levva_silver.clientes)
# MAGIC c.* EXCEPT (c.customer_code),
# MAGIC
# MAGIC -- Atributos de vendedor / canal / região
# MAGIC v.seller_name AS vendedor_nome,
# MAGIC v.status AS vendedor_status,
# MAGIC ca.canal_id,
# MAGIC r.regiao_nome,
# MAGIC r.estado AS regiao_estado,
# MAGIC
# MAGIC -- Métricas do pedido
# MAGIC p.gross_amount,
# MAGIC p.discount_amount,
# MAGIC p.net_amount AS valor_liquido,
# MAGIC p.status_canonico,
# MAGIC p.payment_method,
# MAGIC p.payment_status,
# MAGIC
# MAGIC -- Métricas derivadas (item)
# MAGIC COALESCE(i.qtd_itens, 0) AS qtd_itens,
# MAGIC COALESCE(i.qtd_skus_distintos, 0) AS qtd_skus_distintos,
# MAGIC
# MAGIC -- Flags operacionais
# MAGIC (p.status_canonico = 'CANCELADO') AS flag_cancelado,
# MAGIC COALESCE(e.flag_atrasado, false) AS flag_atrasado,
# MAGIC COALESCE(o.flag_com_ocorrencia, false) AS flag_com_ocorrencia,
# MAGIC COALESCE(o.qtd_ocorrencias, 0) AS qtd_ocorrencias,
# MAGIC COALESCE(o.severity_score_total, 0) AS severity_score_total,
# MAGIC
# MAGIC -- Métricas de entrega
# MAGIC e.lead_time_dias,
# MAGIC e.atraso_dias,
# MAGIC e.delivery_status
# MAGIC
# MAGIC FROM workspace.case_levva_gold.fact_pedido p
# MAGIC LEFT JOIN workspace.case_levva_gold.dim_data d ON p.data_id = d.data_id
# MAGIC LEFT JOIN workspace.case_levva_gold.dim_cliente c ON p.customer_code = c.customer_code
# MAGIC LEFT JOIN workspace.case_levva_gold.dim_vendedor v ON p.seller_id = v.seller_id
# MAGIC LEFT JOIN workspace.case_levva_gold.dim_canal ca ON v.canal_id = ca.canal_id
# MAGIC LEFT JOIN workspace.case_levva_gold.dim_regiao r ON v.regional_code = r.regional_code
# MAGIC LEFT JOIN (
# MAGIC SELECT
# MAGIC order_id,
# MAGIC COUNT(*) AS qtd_itens,
# MAGIC COUNT(DISTINCT product_code) AS qtd_skus_distintos
# MAGIC FROM workspace.case_levva_gold.fact_item
# MAGIC GROUP BY order_id
# MAGIC ) i ON p.order_id = i.order_id
# MAGIC LEFT JOIN (
# MAGIC SELECT
# MAGIC order_id,
# MAGIC MAX(CASE WHEN atraso_dias > 0 THEN true ELSE false END) AS flag_atrasado,
# MAGIC AVG(lead_time_dias) AS lead_time_dias,
# MAGIC MAX(atraso_dias) AS atraso_dias,
# MAGIC FIRST(delivery_status) AS delivery_status
# MAGIC FROM workspace.case_levva_gold.fact_entrega
# MAGIC GROUP BY order_id
# MAGIC ) e ON p.order_id = e.order_id
# MAGIC LEFT JOIN (
# MAGIC SELECT
# MAGIC order_id,
# MAGIC true AS flag_com_ocorrencia,
# MAGIC COUNT(*) AS qtd_ocorrencias,
# MAGIC SUM(severity_score) AS severity_score_total
# MAGIC FROM workspace.case_levva_gold.fact_ocorrencia
# MAGIC GROUP BY order_id
# MAGIC ) o ON p.order_id = o.order_id;

# COMMAND ----------

# Validar que a view foi criada
print("[OK] workspace.case_levva_gold.vw_kpi_business criada.")
spark.sql("SELECT COUNT(*) AS total_pedidos FROM workspace.case_levva_gold.vw_kpi_business").show()
spark.sql("SELECT * FROM workspace.case_levva_gold.vw_kpi_business LIMIT 5").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check — KPIs principais

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC ano_mes,
# MAGIC COUNT(*) AS qtd_pedidos,
# MAGIC ROUND(SUM(valor_liquido), 2) AS receita_liquida,
# MAGIC ROUND(AVG(valor_liquido), 2) AS ticket_medio,
# MAGIC SUM(CASE WHEN flag_cancelado THEN 1 ELSE 0 END) AS pedidos_cancelados,
# MAGIC ROUND(100.0 * SUM(CASE WHEN flag_cancelado THEN 1 ELSE 0 END) / COUNT(*), 2) AS taxa_cancelamento_pct,
# MAGIC SUM(CASE WHEN flag_atrasado THEN 1 ELSE 0 END) AS pedidos_atrasados,
# MAGIC ROUND(100.0 * SUM(CASE WHEN flag_atrasado THEN 1 ELSE 0 END) / COUNT(*), 2) AS taxa_atraso_pct,
# MAGIC SUM(CASE WHEN flag_com_ocorrencia THEN 1 ELSE 0 END) AS pedidos_com_ocorrencia
# MAGIC FROM workspace.case_levva_gold.vw_kpi_business
# MAGIC GROUP BY ano_mes
# MAGIC ORDER BY ano_mes;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 regiões por receita
# MAGIC SELECT
# MAGIC regiao_nome,
# MAGIC COUNT(*) AS qtd_pedidos,
# MAGIC ROUND(SUM(valor_liquido), 2) AS receita_liquida,
# MAGIC ROUND(AVG(valor_liquido), 2) AS ticket_medio
# MAGIC FROM workspace.case_levva_gold.vw_kpi_business
# MAGIC WHERE NOT flag_cancelado
# MAGIC GROUP BY regiao_nome
# MAGIC ORDER BY receita_liquida DESC;
