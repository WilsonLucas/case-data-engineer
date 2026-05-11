# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela: workspace.case_levva_silver.canais
# MAGIC ## Objetivo:
# MAGIC Normalizar o cadastro de canais comerciais a partir do bronze, resolvendo issues estruturais detectadas em `00_exploration`: caps mista em `id_canal`/`tipo_canal`/`ativo`, duplicata conflitante CH05 (`E-commerce` vs `ecommerce`), nulls em `nome_canal` (CH06) e `ativo` (CH07). Saída pronta para servir como `dim_canal` no gold.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `workspace.case_levva_bronze.canais` | Cadastro bruto de canais (8 linhas, 5 colunas, ingestado de `comercial_canais.xlsx` sheet `canais`) |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook (esqueleto) |
# MAGIC | 2026-05-10 | Wilson Lucas | Schema real capturado: dedup CH05 conflitante, normalização case + boolean ativo, DQ flags por categoria |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC: schema `workspace.case_levva_silver` |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

SILVER_SCHEMA = "workspace.case_levva_silver"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

# COMMAND ----------

df_bronze = spark.table("workspace.case_levva_bronze.canais")
print(f"[BRONZE] Linhas: {df_bronze.count()} | Colunas: {df_bronze.columns}")
df_bronze.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Normalização campo a campo

# COMMAND ----------

df_norm = (
    df_bronze.withColumn("id_canal", F.upper(F.trim(F.col("id_canal"))))
    .withColumn("nome_canal", F.initcap(F.trim(F.col("nome_canal"))))
    .withColumn("tipo_canal", F.upper(F.trim(F.col("tipo_canal"))))
    .withColumn(
        "ativo_bool",
        F.when(F.upper(F.trim(F.col("ativo"))).isin("SIM", "S", "TRUE", "1"), F.lit(True))
        .when(F.upper(F.trim(F.col("ativo"))).isin("NAO", "NÃO", "N", "FALSE", "0"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean")),
    )
    .withColumn("observacao", F.trim(F.col("observacao")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Detecção de duplicatas (antes de dedup)
# MAGIC
# MAGIC Marcamos quais `id_canal` têm mais de 1 linha após normalização para que `_dq_reasons` carregue essa informação no registro que sobrevive ao dedup.

# COMMAND ----------

dup_ids = df_norm.groupBy("id_canal").count().filter(F.col("count") > 1).select("id_canal")

df_flagged = df_norm.join(
    dup_ids.withColumn("_is_duplicate", F.lit(True)),
    on="id_canal",
    how="left",
).withColumn("_is_duplicate", F.coalesce(F.col("_is_duplicate"), F.lit(False)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dedup
# MAGIC
# MAGIC Ordenação que privilegia o registro canônico:
# MAGIC 1. `observacao` null primeiro (sinal de "sem conflito flagado pela equipe que exportou").
# MAGIC 2. `_record_id` ascendente (primeiro registro físico).

# COMMAND ----------

window_dedup = Window.partitionBy("id_canal").orderBy(
    F.col("observacao").asc_nulls_first(),
    F.col("_record_id").asc(),
)

df_dedup = df_flagged.withColumn("_rn", F.row_number().over(window_dedup)).filter(F.col("_rn") == 1).drop("_rn")

print(f"[DEDUP] {df_flagged.count()} -> {df_dedup.count()} (esperado: 8 -> 7)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. DQ flags

# COMMAND ----------

df_dq = df_dedup.withColumn(
    "_dq_reasons",
    F.array_compact(
        F.array(
            F.when(F.col("id_canal").isNull() | (F.col("id_canal") == ""), F.lit("id_canal ausente")),
            F.when(F.col("nome_canal").isNull() | (F.col("nome_canal") == ""), F.lit("nome_canal ausente")),
            F.when(F.col("ativo_bool").isNull(), F.lit("ativo nao mapeavel")),
            F.when(F.col("_is_duplicate"), F.lit("duplicata conflitante detectada (mantido registro canonico)")),
        )
    ),
).withColumn(
    "_dq_status",
    F.when(
        F.array_contains(F.col("_dq_reasons"), "id_canal ausente")
        | F.array_contains(F.col("_dq_reasons"), "nome_canal ausente"),
        F.lit("rejected"),
    )
    .when(F.size("_dq_reasons") == 0, F.lit("clean"))
    .otherwise(F.lit("warning")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Seleção final + escrita

# COMMAND ----------

df_final = df_dq.select(
    F.col("id_canal"),
    F.col("nome_canal"),
    F.col("tipo_canal"),
    F.col("ativo_bool").alias("ativo"),
    F.col("observacao"),
    F.col("_source_file"),
    F.col("_ingestion_timestamp"),
    F.col("_record_id"),
    F.col("_dq_status"),
    F.col("_dq_reasons"),
)

(
    df_final.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.canais")
)

print(f"\n[OK] {SILVER_SCHEMA}.canais gravada: {spark.table(f'{SILVER_SCHEMA}.canais').count()} linhas.")

print("\n[DQ] Distribuição de status:")
spark.table(f"{SILVER_SCHEMA}.canais").groupBy("_dq_status").count().orderBy("_dq_status").show()

print("\n[DQ] Conteúdo final:")
spark.table(f"{SILVER_SCHEMA}.canais").show(20, truncate=False)
