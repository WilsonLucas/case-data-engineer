# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela: workspace.case_levva_silver.ocorrencias
# MAGIC ## Objetivo:
# MAGIC Normalizar tickets de atendimento a partir do bronze. Parseia `created_at` em 5 formatos possíveis, padroniza enums (`event_type`, `severity`, `status`) com defaults para nulls, deriva `severity_score` numérico (HIGH=3, MEDIUM=2, LOW=1) para análises ranqueadas no gold. Saída pronta para servir como `fact_ocorrencia`.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `workspace.case_levva_bronze.ocorrencias` | Tickets brutos (~269 linhas, ingestados de `atendimento_ocorrencias.ndjson`) |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC + troca `try_to_timestamp` para `F.expr` (ANSI mode); escape correto de `''T''` em string SQL |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

SILVER_SCHEMA = "workspace.case_levva_silver"


def parse_multi_format_timestamp(col):
    return F.coalesce(
        F.expr(f"try_to_timestamp(replace({col}, 'T', ' '), 'yyyy-MM-dd HH:mm:ss')"),
        F.expr(f"try_to_timestamp({col}, 'yyyy-MM-dd HH:mm:ss')"),
        F.expr(f"try_to_timestamp({col}, 'yyyy-MM-dd')"),
        F.expr(f"try_to_timestamp({col}, 'dd/MM/yyyy HH:mm')"),
        F.expr(f"try_to_timestamp({col}, 'dd/MM/yyyy')"),
    )


# COMMAND ----------

df_bronze = spark.table("workspace.case_levva_bronze.ocorrencias")
print(f"[BRONZE] Linhas: {df_bronze.count()}")
df_bronze.show(5, truncate=False)

# COMMAND ----------

# 1. Normalização
df_normalized = (
    df_bronze.withColumn("ticket_id", F.upper(F.trim(F.col("ticket_id"))))
    .withColumn("order_id", F.upper(F.trim(F.col("order_id"))))
    .withColumn("created_at", parse_multi_format_timestamp("created_at"))
    .withColumn(
        "event_type",
        F.when(
            (F.col("event_type").isNull()) | (F.trim(F.col("event_type")) == ""), F.lit("NAO_CLASSIFICADO")
        ).otherwise(F.upper(F.trim(F.col("event_type")))),
    )
    .withColumn(
        "severity",
        F.when((F.col("severity").isNull()) | (F.trim(F.col("severity")) == ""), F.lit("MEDIUM")).otherwise(
            F.upper(F.trim(F.col("severity")))
        ),
    )
    .withColumn(
        "status",
        F.when((F.col("status").isNull()) | (F.trim(F.col("status")) == ""), F.lit("OPEN")).otherwise(
            F.upper(F.trim(F.col("status")))
        ),
    )
)

# 2. Severity score derivado
df_normalized = df_normalized.withColumn(
    "severity_score",
    F.when(F.col("severity") == "HIGH", 3)
    .when(F.col("severity") == "MEDIUM", 2)
    .when(F.col("severity") == "LOW", 1)
    .otherwise(0),
)

# COMMAND ----------

# 3. DQ flags
df_with_dq = df_normalized.withColumn(
    "_dq_reasons",
    F.array_remove(
        F.array(
            F.when(F.col("event_type") == "NAO_CLASSIFICADO", F.lit("event_type ausente")).otherwise(F.lit(None)),
            F.when(F.col("severity") == "MEDIUM", F.lit("severity ausente, default MEDIUM aplicado")).otherwise(
                F.lit(None)
            ),
            F.when(F.col("created_at").isNull(), F.lit("created_at inválido")).otherwise(F.lit(None)),
            F.when(
                ~F.col("event_type").isin("REFUND", "TROCA", "DELAY", "COMPLAINT", "NAO_CLASSIFICADO"),
                F.lit(F.concat(F.lit("event_type não canônico: "), F.col("event_type"))),
            ).otherwise(F.lit(None)),
        ),
        None,
    ),
).withColumn(
    "_dq_status",
    F.when(F.size("_dq_reasons") == 0, F.lit("clean"))
    .when(F.size("_dq_reasons") <= 2, F.lit("warning"))
    .otherwise(F.lit("rejected")),
)

# COMMAND ----------

# 4. Escrita
df_final = df_with_dq.select(
    "ticket_id",
    "order_id",
    "created_at",
    "event_type",
    "severity",
    "severity_score",
    "status",
    "_dq_status",
    "_dq_reasons",
    "_source_file",
    "_ingestion_timestamp",
)

(
    df_final.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.ocorrencias")
)

print(
    f"\n[OK] workspace.case_levva_silver.ocorrencias gravada. {spark.table(f'{SILVER_SCHEMA}.ocorrencias').count()} linhas."
)
spark.table(f"{SILVER_SCHEMA}.ocorrencias").groupBy("_dq_status").count().show()
spark.table(f"{SILVER_SCHEMA}.ocorrencias").groupBy("event_type", "severity").count().orderBy(F.desc("count")).show()
