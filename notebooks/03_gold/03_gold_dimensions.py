# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: 03_gold_dimensions — 6 dimensões
# MAGIC ## Objetivo:
# MAGIC Criar a camada de dimensões do star schema analítico (SCD Type 1) a partir das tabelas silver. Gera `dim_cliente`, `dim_produto`, `dim_canal`, `dim_regiao`, `dim_vendedor` (entidades) e `dim_data` (gerada cobrindo o range temporal dos fatos, com atributos derivados: ano, mês, trimestre, dia da semana, fim de semana). Saída consumida por `fact_*` e por `vw_kpi_business`.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `workspace.case_levva_silver.clientes` | -> `workspace.case_levva_gold.dim_cliente` |
# MAGIC | `workspace.case_levva_silver.produtos` | -> `workspace.case_levva_gold.dim_produto` |
# MAGIC | `workspace.case_levva_silver.canais` | -> `workspace.case_levva_gold.dim_canal` |
# MAGIC | `workspace.case_levva_silver.regioes` | -> `workspace.case_levva_gold.dim_regiao` |
# MAGIC | `workspace.case_levva_silver.vendedores` | -> `workspace.case_levva_gold.dim_vendedor` |
# MAGIC | `workspace.case_levva_silver.pedidos_cabecalho` + `entregas` + `ocorrencias` | Usadas para inferir o range temporal de `dim_data` |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC: schema `workspace.case_levva_gold` |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

GOLD_SCHEMA = "workspace.case_levva_gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_cliente

# COMMAND ----------

df_silver_clientes = spark.table("workspace.case_levva_silver.clientes")
print(f"[INFO] workspace.case_levva_silver.clientes columns: {df_silver_clientes.columns}")

# Schema mínimo (ajustar com colunas reais conforme schema do XLSX)
df_dim_cliente = (
    df_silver_clientes.select(
        F.col("customer_code"),
        *[
            c
            for c in df_silver_clientes.columns
            if c
            not in ("customer_code", "_dq_status", "_dq_reasons", "_source_file", "_ingestion_timestamp", "_record_id")
        ],
    )
    .filter(F.col("customer_code").isNotNull())
    .dropDuplicates(["customer_code"])
)

(
    df_dim_cliente.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_cliente")
)
print(f"[OK] {GOLD_SCHEMA}.dim_cliente: {spark.table(f'{GOLD_SCHEMA}.dim_cliente').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_produto

# COMMAND ----------

df_silver_produtos = spark.table("workspace.case_levva_silver.produtos")

df_dim_produto = (
    df_silver_produtos.select(
        F.col("product_code"),
        F.col("product_name"),
        F.col("category"),
        F.col("subcategory"),
        F.col("is_active"),
        F.col("list_price"),
        F.col("currency"),
        F.col("family"),
        F.col("tags"),
        F.col("updated_at"),
    )
    .filter(F.col("product_code").isNotNull())
    .dropDuplicates(["product_code"])
)

(
    df_dim_produto.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_produto")
)
print(f"[OK] {GOLD_SCHEMA}.dim_produto: {spark.table(f'{GOLD_SCHEMA}.dim_produto').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_canal

# COMMAND ----------

df_silver_canais = spark.table("workspace.case_levva_silver.canais")

df_dim_canal = (
    df_silver_canais.select(
        F.col("canal_id"),
        *[
            c
            for c in df_silver_canais.columns
            if c not in ("canal_id", "_dq_status", "_dq_reasons", "_source_file", "_ingestion_timestamp", "_record_id")
        ],
    )
    .filter(F.col("canal_id").isNotNull())
    .dropDuplicates(["canal_id"])
)

(
    df_dim_canal.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_canal")
)
print(f"[OK] {GOLD_SCHEMA}.dim_canal: {spark.table(f'{GOLD_SCHEMA}.dim_canal').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_regiao

# COMMAND ----------

df_silver_regioes = spark.table("workspace.case_levva_silver.regioes")

df_dim_regiao = df_silver_regioes.select(
    F.col("regional_code"),
    F.col("regional_name").alias("regiao_nome"),
    F.col("state").alias("estado"),
    F.col("manager_name").alias("gestor_nome"),
    F.col("active_flag"),
)

(
    df_dim_regiao.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_regiao")
)
print(f"[OK] {GOLD_SCHEMA}.dim_regiao: {spark.table(f'{GOLD_SCHEMA}.dim_regiao').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_vendedor

# COMMAND ----------

df_silver_vendedores = spark.table("workspace.case_levva_silver.vendedores")

df_dim_vendedor = df_silver_vendedores.select(
    F.col("seller_id"),
    F.col("seller_name"),
    F.col("canal_id"),
    F.col("regional_code"),
    F.col("hire_date"),
    F.col("status"),
)

(
    df_dim_vendedor.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_vendedor")
)
print(f"[OK] {GOLD_SCHEMA}.dim_vendedor: {spark.table(f'{GOLD_SCHEMA}.dim_vendedor').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_data
# MAGIC
# MAGIC Gerada cobrindo o range completo das datas em fatos (pedidos + entregas + ocorrências) com folga de ±30 dias.

# COMMAND ----------

# Determinar range de datas a partir das fontes
date_ranges = spark.sql("""
 SELECT
 LEAST(
 MIN(p.order_date),
 MIN(e.shipped_at::date),
 MIN(o.created_at::date)
 ) AS min_date,
 GREATEST(
 MAX(p.order_date),
 MAX(p.promised_date),
 MAX(e.delivered_at::date),
 MAX(o.created_at::date)
 ) AS max_date
 FROM workspace.case_levva_silver.pedidos_cabecalho p
 LEFT JOIN workspace.case_levva_silver.entregas e ON p.order_id = e.order_id
 LEFT JOIN workspace.case_levva_silver.ocorrencias o ON p.order_id = o.order_id
""").collect()[0]

min_date = date_ranges.min_date
max_date = date_ranges.max_date

print(f"[INFO] Range detectado: {min_date} -> {max_date}")

# Gera dim_data com folga de 30 dias antes e depois
df_dim_data = (
    spark.sql(f"""
 SELECT
 sequence(
 date_sub(to_date('{min_date}'), 30),
 date_add(to_date('{max_date}'), 30),
 interval 1 day
 ) AS dates
 """)
    .select(F.explode("dates").alias("data_completa"))
    .withColumn("data_id", F.date_format("data_completa", "yyyyMMdd").cast("int"))
    .withColumn("ano", F.year("data_completa"))
    .withColumn("mes", F.month("data_completa"))
    .withColumn("dia", F.dayofmonth("data_completa"))
    .withColumn("trimestre", F.quarter("data_completa"))
    .withColumn("mes_nome", F.date_format("data_completa", "MMMM"))
    .withColumn("dia_semana_num", F.dayofweek("data_completa"))
    .withColumn(
        "dia_semana",
        F.when(F.col("dia_semana_num") == 1, "Domingo")
        .when(F.col("dia_semana_num") == 2, "Segunda")
        .when(F.col("dia_semana_num") == 3, "Terça")
        .when(F.col("dia_semana_num") == 4, "Quarta")
        .when(F.col("dia_semana_num") == 5, "Quinta")
        .when(F.col("dia_semana_num") == 6, "Sexta")
        .when(F.col("dia_semana_num") == 7, "Sábado"),
    )
    .withColumn("fim_de_semana", F.col("dia_semana_num").isin(1, 7))
    .withColumn("ano_mes", F.date_format("data_completa", "yyyy-MM"))
    .select(
        "data_id",
        "data_completa",
        "ano",
        "mes",
        "dia",
        "trimestre",
        "mes_nome",
        "dia_semana",
        "dia_semana_num",
        "fim_de_semana",
        "ano_mes",
    )
)

(
    df_dim_data.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_data")
)
print(f"[OK] {GOLD_SCHEMA}.dim_data: {spark.table(f'{GOLD_SCHEMA}.dim_data').count()} linhas")
spark.table(f"{GOLD_SCHEMA}.dim_data").orderBy("data_completa").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo das dimensões

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.case_levva_gold;

# COMMAND ----------

print("\n[GOLD - Dimensões] Resumo:")
print(f"{'Tabela':<25} {'Linhas':>10}")
print("-" * 40)

dims = ["dim_cliente", "dim_produto", "dim_canal", "dim_regiao", "dim_vendedor", "dim_data"]
for d in dims:
    count = spark.table(f"{GOLD_SCHEMA}.{d}").count()
    print(f"{d:<25} {count:>10,}")
