# Databricks notebook source
# MAGIC %md
# MAGIC # Tabelas: workspace.silver.pedidos_cabecalho + pedidos_itens
# MAGIC ## Objetivo:
# MAGIC Normalizar pedidos a partir do bronze, em duas tabelas paralelas (granular pedido e granular item). Trata: 3 formatos de data misturados (`order_date`, `promised_date`, `last_update`), decimal BR (vírgula) -> US (ponto), padronização de `status_order` para canônico (`FATURADO`/`EM_SEPARACAO`/`CANCELADO`/`OUTRO`), parse de `payment_details` JSON aninhado, validação `net = gross - discount`, recalculo de `total_item` para detectar divergências de arredondamento. Saída alimenta `fact_pedido` e `fact_item` no gold.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `workspace.bronze.pedidos_cabecalho` | Cabeçalhos brutos (~403 linhas) com `payment_details` como JSON string |
# MAGIC | `workspace.bronze.pedidos_itens` | Itens brutos (~995 linhas) com `quantity`, `unit_price`, `total_item` como string |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC: schema `workspace.silver` |
# MAGIC | 2026-05-10 | Wilson Lucas | ANSI mode fixes: cast `double->int` em `quantity`/`item_seq`; `try_cast` em decimals via `F.expr`; troca de `F.try_to_*` para `F.expr("try_to_*")` |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

SILVER_SCHEMA = "workspace.silver"


# Função utilitária — multi-format date
def parse_multi_format_date(col_name):
    return F.coalesce(
        F.expr(f"try_to_date({col_name}, 'yyyy-MM-dd')"),
        F.expr(f"try_to_date({col_name}, 'dd/MM/yyyy')"),
        F.expr(f"try_to_timestamp({col_name}, 'dd/MM/yyyy HH:mm')").cast("date"),
    )


def parse_multi_format_timestamp(col_name):
    return F.coalesce(
        F.expr(f"try_to_timestamp(replace({col_name}, 'T', ' '), 'yyyy-MM-dd HH:mm:ss')"),
        F.expr(f"try_to_timestamp({col_name}, 'yyyy-MM-dd HH:mm:ss')"),
        F.expr(f"try_to_timestamp({col_name}, 'yyyy-MM-dd')"),
        F.expr(f"try_to_timestamp({col_name}, 'dd/MM/yyyy HH:mm')"),
        F.expr(f"try_to_timestamp({col_name}, 'dd/MM/yyyy')"),
    )


def br_to_us_decimal(col_name):
    """Converte string com vírgula decimal BR para decimal US.
    Usa try_cast via SQL expr para tolerar valores não-numéricos ('N/A', '-', vazio) -> NULL.
    Em ANSI mode estrito (default em UC), cast direto falharia com CAST_INVALID_INPUT."""
    return F.expr(f"try_cast(replace({col_name}, ',', '.') as decimal(15,2))")


# COMMAND ----------

# MAGIC %md
# MAGIC ## CABEÇALHO

# COMMAND ----------

df_bronze_cab = spark.table("workspace.bronze.pedidos_cabecalho")
print(f"[BRONZE] Cabeçalho: {df_bronze_cab.count()} linhas")

# Schema do payment_details (JSON aninhado)
schema_payment = StructType(
    [
        StructField("method", StringType()),
        StructField("status", StringType()),
        StructField("installments", StringType()),  # pode vir como número, tratamos como string
    ]
)

# 1. Parsing e cast
df_cab_normalized = (
    df_bronze_cab.withColumn("order_id", F.upper(F.trim(F.col("order_id"))))
    .withColumn("customer_code", F.upper(F.trim(F.col("customer_code"))))
    .withColumn("seller_id", F.upper(F.trim(F.col("seller_id"))))
    .withColumn("order_date", parse_multi_format_date("order_date"))
    .withColumn("promised_date", parse_multi_format_date("promised_date"))
    .withColumn("last_update", parse_multi_format_timestamp("last_update"))
    .withColumn("gross_amount", br_to_us_decimal("gross_amount"))
    .withColumn("discount_amount", br_to_us_decimal("discount_amount"))
    .withColumn("net_amount", br_to_us_decimal("net_amount"))
    .withColumn(
        "status_canonico",
        F.when(F.upper(F.trim(F.col("status_order"))) == "FATURADO", F.lit("FATURADO"))
        .when(
            F.upper(F.trim(F.col("status_order"))).isin("EM_SEPARACAO", "EM SEPARACAO", "SEPARANDO"),
            F.lit("EM_SEPARACAO"),
        )
        .when(F.upper(F.trim(F.col("status_order"))).isin("CANCELADO", "CANCELLED"), F.lit("CANCELADO"))
        .otherwise(F.lit("OUTRO")),
    )
    .withColumn("payment_struct", F.from_json(F.col("payment_details"), schema_payment))
    .withColumn("payment_method", F.upper(F.trim(F.col("payment_struct.method"))))
    .withColumn("payment_status", F.upper(F.trim(F.col("payment_struct.status"))))
)

# 2. Validação: net_amount = gross_amount - discount_amount
df_cab_validated = df_cab_normalized.withColumn(
    "net_amount_calculado", F.col("gross_amount") - F.col("discount_amount")
).withColumn("divergencia_net_amount", F.abs(F.col("net_amount") - F.col("net_amount_calculado")) > 0.01)

# 3. DQ flags
df_cab_dq = df_cab_validated.withColumn(
    "_dq_reasons",
    F.array_compact(
        F.array(
            F.when(F.col("order_date").isNull(), F.lit("order_date inválida")).otherwise(F.lit(None)),
            F.when(F.col("net_amount").isNull(), F.lit("net_amount inválido")).otherwise(F.lit(None)),
            F.when(F.col("divergencia_net_amount"), F.lit("net_amount divergente do calculado")).otherwise(
                F.lit(None)
            ),
            F.when(
                F.col("status_canonico") == "OUTRO",
                F.concat(F.lit("status não canônico: "), F.col("status_order")),
            ).otherwise(F.lit(None)),
            F.when(F.col("payment_method").isNull(), F.lit("payment_method ausente")).otherwise(F.lit(None)),
        )
    ),
).withColumn(
    "_dq_status",
    F.when(F.size("_dq_reasons") == 0, F.lit("clean"))
    .when(F.size("_dq_reasons") <= 2, F.lit("warning"))
    .otherwise(F.lit("rejected")),
)

# 4. Schema final
df_cab_final = df_cab_dq.select(
    "order_id",
    "customer_code",
    "seller_id",
    "order_date",
    "promised_date",
    "last_update",
    "gross_amount",
    "discount_amount",
    "net_amount",
    "status_canonico",
    F.col("status_order").alias("status_raw"),
    "payment_method",
    "payment_status",
    "_dq_status",
    "_dq_reasons",
    "_source_file",
    "_ingestion_timestamp",
)

(
    df_cab_final.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.pedidos_cabecalho")
)

print(
    f"\n[OK] workspace.silver.pedidos_cabecalho gravada. {spark.table(f'{SILVER_SCHEMA}.pedidos_cabecalho').count()} linhas."
)
spark.table(f"{SILVER_SCHEMA}.pedidos_cabecalho").groupBy("_dq_status").count().show()
spark.table(f"{SILVER_SCHEMA}.pedidos_cabecalho").groupBy("status_canonico").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ITENS

# COMMAND ----------

df_bronze_itens = spark.table("workspace.bronze.pedidos_itens")
print(f"[BRONZE] Itens: {df_bronze_itens.count()} linhas")

# 1. Cast e normalização
df_itens_normalized = (
    df_bronze_itens.withColumn("order_id", F.upper(F.trim(F.col("order_id"))))
    # Cast double->int é tolerante a "5.0" e "5,0"; ANSI mode estrito (default no UC) rejeita cast direto string->int de "5.0"
    .withColumn("item_seq", F.col("item_seq").cast("double").cast("int"))
    .withColumn("product_code", F.upper(F.trim(F.col("product_code"))))
    .withColumn("quantity", F.col("quantity").cast("double").cast("int"))
    .withColumn("unit_price", br_to_us_decimal("unit_price"))
    .withColumn("total_item", br_to_us_decimal("total_item"))
    .withColumn(
        "item_status",
        F.when(
            (F.col("item_status").isNull()) | (F.trim(F.col("item_status")) == ""), F.lit("NAO_INFORMADO")
        ).otherwise(F.upper(F.trim(F.col("item_status")))),
    )
)

# 2. Validação: total_item = quantity × unit_price
df_itens_validated = df_itens_normalized.withColumn(
    "total_item_calculado", (F.col("quantity") * F.col("unit_price")).cast("decimal(15,2)")
).withColumn("divergencia_total", F.abs(F.col("total_item") - F.col("total_item_calculado")) > 0.01)

# 3. DQ flags
df_itens_dq = df_itens_validated.withColumn(
    "_dq_reasons",
    F.array_compact(
        F.array(
            F.when(F.col("quantity").isNull(), F.lit("quantity inválida")).otherwise(F.lit(None)),
            F.when(F.col("unit_price").isNull(), F.lit("unit_price inválido")).otherwise(F.lit(None)),
            F.when(F.col("item_status") == "NAO_INFORMADO", F.lit("item_status ausente")).otherwise(F.lit(None)),
            F.when(F.col("divergencia_total"), F.lit("total_item divergente de quantity×unit_price")).otherwise(
                F.lit(None)
            ),
        )
    ),
).withColumn("_dq_status", F.when(F.size("_dq_reasons") == 0, F.lit("clean")).otherwise(F.lit("warning")))

# 4. Escrita
df_itens_final = df_itens_dq.select(
    "order_id",
    "item_seq",
    "product_code",
    "quantity",
    "unit_price",
    "total_item",
    "total_item_calculado",
    "divergencia_total",
    "item_status",
    "_dq_status",
    "_dq_reasons",
    "_source_file",
    "_ingestion_timestamp",
)

(
    df_itens_final.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.pedidos_itens")
)

print(
    f"\n[OK] workspace.silver.pedidos_itens gravada. {spark.table(f'{SILVER_SCHEMA}.pedidos_itens').count()} linhas."
)
spark.table(f"{SILVER_SCHEMA}.pedidos_itens").groupBy("_dq_status").count().show()
