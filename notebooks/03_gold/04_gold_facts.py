# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: 04_gold_facts — 4 fatos
# MAGIC ## Objetivo:
# MAGIC Criar a camada de fatos do star schema analítico a partir das silvers normalizadas. Granularidade explícita por tabela:
# MAGIC - `fact_pedido` (1 linha por pedido) — métricas: `gross_amount`, `discount_amount`, `net_amount`, FKs para todas as dimensões.
# MAGIC - `fact_item` (1 linha por item) — métricas: `quantity`, `unit_price`, `total_item`.
# MAGIC - `fact_entrega` (1 linha por entrega) — métricas: `cost`, `lead_time_dias`, `atraso_dias`, `on_time_flag`.
# MAGIC - `fact_ocorrencia` (1 linha por ticket) — métricas: `severity_score`, contagem agregável.
# MAGIC
# MAGIC FKs apontam para chaves naturais das dimensões (`customer_code`, `seller_id`, etc.) e `data_id` no formato `yyyyMMdd` para join com `dim_data`.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `workspace.silver.pedidos_cabecalho` | -> `fact_pedido` |
# MAGIC | `workspace.silver.pedidos_itens` | -> `fact_item` |
# MAGIC | `workspace.silver.entregas` | -> `fact_entrega` |
# MAGIC | `workspace.silver.ocorrencias` | -> `fact_ocorrencia` |
# MAGIC | `workspace.gold.dim_*` | Lookups para FKs e validação referencial |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC: schema `workspace.gold` |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

GOLD_SCHEMA = "workspace.gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_pedido

# COMMAND ----------

df_silver_pedidos = spark.table("workspace.silver.pedidos_cabecalho")

df_fact_pedido = df_silver_pedidos.select(
    F.col("order_id"),
    F.col("customer_code"),
    F.col("seller_id"),
    F.date_format(F.col("order_date"), "yyyyMMdd").cast("int").alias("data_id"),
    F.date_format(F.col("promised_date"), "yyyyMMdd").cast("int").alias("data_promessa_id"),
    F.col("gross_amount"),
    F.col("discount_amount"),
    F.col("net_amount"),
    F.col("status_canonico"),
    F.col("payment_method"),
    F.col("payment_status"),
)

(
    df_fact_pedido.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.fact_pedido")
)
print(f"[OK] {GOLD_SCHEMA}.fact_pedido: {spark.table(f'{GOLD_SCHEMA}.fact_pedido').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_item

# COMMAND ----------

df_silver_itens = spark.table("workspace.silver.pedidos_itens")

df_fact_item = df_silver_itens.select(
    F.col("order_id"),
    F.col("item_seq"),
    F.col("product_code"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("total_item"),
    F.col("total_item_calculado"),
    F.col("divergencia_total"),
    F.col("item_status"),
)

(
    df_fact_item.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.fact_item")
)
print(f"[OK] {GOLD_SCHEMA}.fact_item: {spark.table(f'{GOLD_SCHEMA}.fact_item').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_entrega

# COMMAND ----------

df_silver_entregas = spark.table("workspace.silver.entregas")

df_fact_entrega = df_silver_entregas.select(
    F.col("delivery_id"),
    F.col("order_id"),
    F.date_format(F.col("shipped_at").cast("date"), "yyyyMMdd").cast("int").alias("data_envio_id"),
    F.date_format(F.col("delivered_at").cast("date"), "yyyyMMdd").cast("int").alias("data_entrega_id"),
    F.col("carrier_name"),
    F.col("mode"),
    F.col("delivery_status"),
    F.col("destination_state"),
    F.col("destination_city"),
    F.col("cost"),
    F.col("lead_time_dias"),
    F.col("atraso_dias"),
    F.col("on_time_flag"),
)

(
    df_fact_entrega.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.fact_entrega")
)
print(f"[OK] {GOLD_SCHEMA}.fact_entrega: {spark.table(f'{GOLD_SCHEMA}.fact_entrega').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_ocorrencia

# COMMAND ----------

df_silver_ocorr = spark.table("workspace.silver.ocorrencias")

df_fact_ocorr = df_silver_ocorr.select(
    F.col("ticket_id"),
    F.col("order_id"),
    F.date_format(F.col("created_at").cast("date"), "yyyyMMdd").cast("int").alias("data_id"),
    F.col("event_type"),
    F.col("severity"),
    F.col("severity_score"),
    F.col("status"),
)

(
    df_fact_ocorr.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.fact_ocorrencia")
)
print(f"[OK] {GOLD_SCHEMA}.fact_ocorrencia: {spark.table(f'{GOLD_SCHEMA}.fact_ocorrencia').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.gold;

# COMMAND ----------

print("\n[GOLD - Fatos] Resumo:")
print(f"{'Tabela':<25} {'Linhas':>10}")
print("-" * 40)

facts = ["fact_pedido", "fact_item", "fact_entrega", "fact_ocorrencia"]
for f_name in facts:
    count = spark.table(f"{GOLD_SCHEMA}.{f_name}").count()
    print(f"{f_name:<25} {count:>10,}")
