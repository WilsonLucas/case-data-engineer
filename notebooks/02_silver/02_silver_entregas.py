# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela: workspace.silver.entregas
# MAGIC ## Objetivo:
# MAGIC Normalizar dados de logística a partir do bronze. Aplana estruturas JSON aninhadas (`carrier`, `timestamps`, `destination`), padroniza `delivery_status` para PT canônico (`ENTREGUE`/`EM_TRANSITO`/`ATRASADO`), parseia timestamps multi-formato, e calcula métricas operacionais (`lead_time_dias`, `atraso_dias`, `on_time_flag`) via join com `pedidos_cabecalho` para obter `promised_date`. Saída pronta para servir como `fact_entrega` no gold.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `workspace.bronze.entregas` | Logística bruta com structs serializados como JSON string (~1700 linhas, ingestada de `logistica_entregas.json`) |
# MAGIC | `workspace.silver.pedidos_cabecalho` | Usada para enriquecer com `promised_date` e calcular `atraso_dias` (lookup join) |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC + troca `try_to_timestamp` para `F.expr` (ANSI mode); `try_cast` em `cost` para tolerar valores não-numéricos |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

SILVER_SCHEMA = "workspace.silver"

# COMMAND ----------

# Schemas para parse
schema_carrier = StructType(
    [
        StructField("name", StringType()),
        StructField("mode", StringType()),
    ]
)

schema_timestamps = StructType(
    [
        StructField("shipped_at", StringType()),
        StructField("delivered_at", StringType()),
    ]
)

schema_destination = StructType(
    [
        StructField("state", StringType()),
        StructField("city", StringType()),
    ]
)


# Função utilitária — multi-format timestamp
def parse_multi_format_timestamp(col):
    return F.coalesce(
        F.expr(f"try_to_timestamp(replace({col}, 'T', ' '), 'yyyy-MM-dd HH:mm:ss')"),
        F.expr(f"try_to_timestamp({col}, 'yyyy-MM-dd HH:mm:ss')"),
        F.expr(f"try_to_timestamp({col}, 'yyyy-MM-dd')"),
        F.expr(f"try_to_timestamp({col}, 'dd/MM/yyyy HH:mm')"),
        F.expr(f"try_to_timestamp({col}, 'dd/MM/yyyy')"),
    )


# COMMAND ----------

df_bronze = spark.table("workspace.bronze.entregas")
print(f"[BRONZE] Linhas: {df_bronze.count()}")

# 1. Parse JSONs aninhados
df_parsed = (
    df_bronze.withColumn("carrier_struct", F.from_json("carrier_json", schema_carrier))
    .withColumn("timestamps_struct", F.from_json("timestamps_json", schema_timestamps))
    .withColumn("destination_struct", F.from_json("destination_json", schema_destination))
    .select(
        F.col("delivery_id"),
        F.col("order_ref").alias("order_id"),
        F.col("delivery_status").alias("delivery_status_raw"),
        F.col("cost"),
        F.col("carrier_struct.name").alias("carrier_name_raw"),
        F.col("carrier_struct.mode").alias("mode"),
        F.col("timestamps_struct.shipped_at").alias("shipped_at_raw"),
        F.col("timestamps_struct.delivered_at").alias("delivered_at_raw"),
        F.col("destination_struct.state").alias("destination_state"),
        F.col("destination_struct.city").alias("destination_city"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
    )
)

# COMMAND ----------

# 2. Normalizações
df_normalized = (
    df_parsed.withColumn("delivery_id", F.upper(F.trim(F.col("delivery_id"))))
    .withColumn("order_id", F.upper(F.trim(F.col("order_id"))))
    # try_cast tolera 'N/A' / vazio -> NULL (ANSI mode rejeitaria cast direto)
    .withColumn("cost", F.expr("try_cast(replace(cost, ',', '.') as decimal(15,2))"))
    .withColumn(
        "carrier_name",
        F.when(
            (F.col("carrier_name_raw").isNull()) | (F.trim(F.col("carrier_name_raw")) == ""),
            F.lit("TRANSPORTADORA_NAO_INFORMADA"),
        ).otherwise(F.upper(F.trim(F.col("carrier_name_raw")))),
    )
    .withColumn("mode", F.upper(F.trim(F.col("mode"))))
    .withColumn(
        "delivery_status",
        F.when(F.upper(F.trim(F.col("delivery_status_raw"))).isin("DELIVERED", "ENTREGUE"), F.lit("ENTREGUE"))
        .when(F.upper(F.trim(F.col("delivery_status_raw"))).isin("ATRASADO", "DELAYED"), F.lit("ATRASADO"))
        .when(
            F.upper(F.trim(F.col("delivery_status_raw"))).isin("EM_TRANSITO", "IN_TRANSIT", "EM TRANSITO"),
            F.lit("EM_TRANSITO"),
        )
        .when(F.upper(F.trim(F.col("delivery_status_raw"))).isin("CANCELADO", "CANCELLED"), F.lit("CANCELADO"))
        .otherwise(F.lit("OUTRO")),
    )
    .withColumn("destination_state", F.upper(F.trim(F.col("destination_state"))))
    .withColumn("destination_city", F.initcap(F.trim(F.col("destination_city"))))
    .withColumn("shipped_at", parse_multi_format_timestamp("shipped_at_raw"))
    .withColumn("delivered_at", parse_multi_format_timestamp("delivered_at_raw"))
)

# 3. Cálculo de lead_time
df_with_lead = df_normalized.withColumn(
    "lead_time_dias", F.datediff(F.col("delivered_at").cast("date"), F.col("shipped_at").cast("date"))
)

# COMMAND ----------

# 4. Join com pedidos para calcular atraso
df_pedidos = spark.table(f"{SILVER_SCHEMA}.pedidos_cabecalho").select("order_id", "promised_date")

df_with_atraso = (
    df_with_lead.join(df_pedidos, on="order_id", how="left")
    .withColumn("atraso_dias", F.datediff(F.col("delivered_at").cast("date"), F.col("promised_date")))
    .withColumn(
        "on_time_flag",
        F.when(F.col("atraso_dias") <= 0, True).when(F.col("atraso_dias").isNull(), None).otherwise(False),
    )
)

# COMMAND ----------

# 5. DQ flags
df_with_dq = df_with_atraso.withColumn(
    "_dq_reasons",
    F.array_compact(
        F.array(
            F.when(
                F.col("delivery_status") == "OUTRO",
                F.lit(F.concat(F.lit("delivery_status não canônico: "), F.col("delivery_status_raw"))),
            ).otherwise(F.lit(None)),
            F.when(F.col("carrier_name") == "TRANSPORTADORA_NAO_INFORMADA", F.lit("carrier.name ausente")).otherwise(
                F.lit(None)
            ),
            F.when(F.col("shipped_at").isNull(), F.lit("shipped_at inválido")).otherwise(F.lit(None)),
            F.when(
                F.col("delivered_at").isNull() & F.col("delivery_status").isin("ENTREGUE", "ATRASADO"),
                F.lit("delivered_at ausente apesar de status entregue"),
            ).otherwise(F.lit(None)),
            F.when(F.col("promised_date").isNull(), F.lit("pedido referenciado sem promised_date")).otherwise(
                F.lit(None)
            ),
        )
    ),
).withColumn(
    "_dq_status",
    F.when(F.size("_dq_reasons") == 0, F.lit("clean"))
    .when(F.col("delivery_status") == "OUTRO", F.lit("rejected"))
    .otherwise(F.lit("warning")),
)

# COMMAND ----------

# 6. Schema final
df_final = df_with_dq.select(
    "delivery_id",
    "order_id",
    "carrier_name",
    "mode",
    "delivery_status",
    "shipped_at",
    "delivered_at",
    "destination_state",
    "destination_city",
    "cost",
    "lead_time_dias",
    "atraso_dias",
    "on_time_flag",
    "_dq_status",
    "_dq_reasons",
    "_source_file",
    "_ingestion_timestamp",
)

(
    df_final.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.entregas")
)

print(
    f"\n[OK] workspace.silver.entregas gravada. {spark.table(f'{SILVER_SCHEMA}.entregas').count()} linhas."
)
spark.table(f"{SILVER_SCHEMA}.entregas").groupBy("_dq_status").count().show()
spark.table(f"{SILVER_SCHEMA}.entregas").groupBy("delivery_status").count().show()
