# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela: workspace.silver.regioes
# MAGIC ## Objetivo:
# MAGIC Normalizar tabela legada de regiões (pipe-delimited) aplicando lookup canônico de códigos (S/Sul -> S, SE/Sudeste -> SE) para resolver duplicatas semânticas. Dedup por código mantendo o registro mais "vivo" (`active_flag=1`), correções de capitalização em `manager_name`, e cast de `active_flag` para boolean. Saída pronta para servir como `dim_regiao` no gold.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `workspace.bronze.regioes` | Cadastro legado de regiões (~9 linhas brutas, ingestadas de `legado_regioes_pipe.txt` com `\|` como separador) |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC: schema `workspace.silver` |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

SILVER_SCHEMA = "workspace.silver"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

# COMMAND ----------

# Leitura do Bronze
df_bronze = spark.table("workspace.bronze.regioes")
print(f"[BRONZE] Linhas lidas: {df_bronze.count()}")
df_bronze.show(20, truncate=False)

# COMMAND ----------

# 1. Normalização de regional_code
# Mapeamento canônico — qualquer variação textual mapeia para uma das 6 categorias oficiais
df_normalized = df_bronze.withColumn("regional_code_raw", F.col("regional_code")).withColumn(
    "regional_code",
    F.when(F.upper(F.trim(F.col("regional_code"))).isin("S", "SUL"), F.lit("S"))
    .when(F.upper(F.trim(F.col("regional_code"))).isin("SE", "SUDESTE"), F.lit("SE"))
    .when(F.upper(F.trim(F.col("regional_code"))).isin("N", "NORTE"), F.lit("N"))
    .when(F.upper(F.trim(F.col("regional_code"))).isin("NE", "NORDESTE"), F.lit("NE"))
    .when(F.upper(F.trim(F.col("regional_code"))).isin("CO", "CENTRO_OESTE", "CENTRO-OESTE"), F.lit("CO"))
    .when(F.upper(F.trim(F.col("regional_code"))).isin("XX", ""), F.lit("XX"))
    .otherwise(F.upper(F.trim(F.col("regional_code")))),  # outras variações que precisem investigação
)

# 2. Limpeza de manager_name (INITCAP + correção de "sao paulo")
df_normalized = df_normalized.withColumn(
    "manager_name",
    F.when(F.lower(F.col("manager_name")) == "sao paulo", F.lit("São Paulo")).otherwise(
        F.initcap(F.trim(F.col("manager_name")))
    ),
)

# 3. Cast active_flag para boolean
df_normalized = df_normalized.withColumn(
    "active_flag",
    F.when(F.col("active_flag").isin("1", "true", "True", "TRUE"), True)
    .when(F.col("active_flag").isin("0", "false", "False", "FALSE"), False)
    .otherwise(None),
)

# 4. Padronizar regional_name e state
df_normalized = df_normalized.withColumn("regional_name", F.initcap(F.trim(F.col("regional_name")))).withColumn(
    "state", F.upper(F.trim(F.col("state")))
)

# 5. Override label da região XX para "Não informada"
df_normalized = df_normalized.withColumn(
    "regional_name", F.when(F.col("regional_code") == "XX", F.lit("Não informada")).otherwise(F.col("regional_name"))
)

df_normalized.show(20, truncate=False)

# COMMAND ----------

# 6. Dedup — agrupa por regional_code canônico, mantém o registro com active_flag=true (preferência)
window_dedup = Window.partitionBy("regional_code").orderBy(
    F.col("active_flag").desc_nulls_last(), F.col("_ingestion_timestamp").desc()
)

df_deduped = df_normalized.withColumn("_rn", F.row_number().over(window_dedup)).filter(F.col("_rn") == 1).drop("_rn")

print(f"[DEDUP] Linhas antes: {df_normalized.count()} -> depois: {df_deduped.count()}")

# COMMAND ----------

# 7. DQ flags
df_with_dq = (
    df_deduped.withColumn(
        "_dq_reasons",
        F.array_compact(
            F.array(
                F.when(F.col("regional_code") == "XX", F.lit("regional_code=XX (placeholder)")).otherwise(F.lit(None)),
                F.when(F.col("active_flag").isNull(), F.lit("active_flag não pôde ser convertido")).otherwise(
                    F.lit(None)
                ),
                F.when(
                    F.col("regional_code_raw") != F.col("regional_code"),
                    F.lit(F.concat(F.lit("regional_code normalizado de "), F.col("regional_code_raw"))),
                ).otherwise(F.lit(None)),
            )
        ),
    )
    .withColumn(
        "_dq_status",
        F.when(F.size("_dq_reasons") == 0, F.lit("clean"))
        .when(F.size("_dq_reasons") <= 2, F.lit("warning"))
        .otherwise(F.lit("warning")),  # nenhum critério atual gera rejected aqui
    )
    .drop("regional_code_raw")
)

df_with_dq.show(20, truncate=False)

# COMMAND ----------

# 8. Schema final + escrita
df_final = df_with_dq.select(
    "regional_code",
    "regional_name",
    "state",
    "manager_name",
    "active_flag",
    "_dq_status",
    "_dq_reasons",
    "_source_file",
    "_ingestion_timestamp",
)

(
    df_final.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.regioes")
)

print(
    f"\n[OK] workspace.silver.regioes gravada. Total: {spark.table(f'{SILVER_SCHEMA}.regioes').count()} linhas."
)

# Resumo DQ
print("\n[DQ Summary]")
spark.table(f"{SILVER_SCHEMA}.regioes").groupBy("_dq_status").count().show()
