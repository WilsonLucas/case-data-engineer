# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela: workspace.silver.produtos
# MAGIC ## Objetivo:
# MAGIC Normalizar cadastro de produtos a partir do bronze. Aplana 3 structs serializados como JSON string (`product`, `pricing`, `attributes`) em colunas tabulares, padroniza `status` para boolean `is_active`, parseia `list_price` com decimal BR, e valida `currency`. Saída pronta para servir como `dim_produto` no gold.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `workspace.bronze.produtos` | Catálogo bruto (~65 linhas, ingestado de `cadastro_produtos_api_dump.json` com structs aninhados serializados) |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC + `try_cast` em `list_price` para tolerar 'N/A' (ANSI mode); troca `try_to_timestamp` para `F.expr` |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

SILVER_SCHEMA = "workspace.silver"

# COMMAND ----------

# Schemas explícitos para parse dos JSONs
schema_product = StructType(
    [
        StructField("product_id", StringType()),
        StructField("name", StringType()),
        StructField("category", StringType()),
        StructField("subcategory", StringType()),
        StructField("status", StringType()),
    ]
)

schema_pricing = StructType(
    [
        StructField("list_price", StringType()),  # vem como string ou número, tratamos como string para depois castar
        StructField("currency", StringType()),
    ]
)

schema_attributes = StructType(
    [
        StructField("family", StringType()),
        StructField("tags", ArrayType(StringType())),
    ]
)

# COMMAND ----------

# Leitura do Bronze
df_bronze = spark.table("workspace.bronze.produtos")
print(f"[BRONZE] Linhas lidas: {df_bronze.count()}")

# COMMAND ----------

# 1. Parse dos JSONs aninhados
df_parsed = (
    df_bronze.withColumn("product_struct", F.from_json("product_json", schema_product))
    .withColumn("pricing_struct", F.from_json("pricing_json", schema_pricing))
    .withColumn("attributes_struct", F.from_json("attributes_json", schema_attributes))
    .select(
        F.col("product_struct.product_id").alias("product_code"),
        F.col("product_struct.name").alias("product_name"),
        F.col("product_struct.category").alias("category"),
        F.col("product_struct.subcategory").alias("subcategory"),
        F.col("product_struct.status").alias("status_raw"),
        F.col("pricing_struct.list_price").alias("list_price_raw"),
        F.col("pricing_struct.currency").alias("currency"),
        F.col("attributes_struct.family").alias("family"),
        F.col("attributes_struct.tags").alias("tags"),
        F.col("updated_at").alias("updated_at_raw"),
        F.col("_source_file"),
        F.col("_ingestion_timestamp"),
    )
)

df_parsed.show(5, truncate=False)

# COMMAND ----------

# 2. Normalização e cast
df_normalized = (
    df_parsed.withColumn(
        "is_active",
        F.when(F.upper(F.trim(F.col("status_raw"))) == "ATIVO", True)
        .when(F.upper(F.trim(F.col("status_raw"))) == "INATIVO", False)
        .otherwise(None),
    )
    .withColumn(
        "list_price",
        # try_cast tolera 'N/A' / vazio -> NULL (ANSI mode rejeitaria cast direto)
        F.expr("try_cast(replace(list_price_raw, ',', '.') as decimal(15,2))"),
    )
    .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    .withColumn("category", F.initcap(F.trim(F.col("category"))))
    .withColumn("subcategory", F.initcap(F.trim(F.col("subcategory"))))
    .withColumn(
        "updated_at",
        F.coalesce(
            F.expr("try_to_timestamp(replace(updated_at_raw, 'T', ' '), 'yyyy-MM-dd HH:mm:ss')"),
            F.expr("try_to_timestamp(updated_at_raw, 'yyyy-MM-dd HH:mm:ss')"),
            F.expr("try_to_timestamp(updated_at_raw, 'yyyy-MM-dd')"),
        ),
    )
)

# COMMAND ----------

# 3. DQ flags
df_with_dq = df_normalized.withColumn(
    "_dq_reasons",
    F.array_compact(
        F.array(
            F.when(F.col("is_active").isNull(), F.lit("status não reconhecido")).otherwise(F.lit(None)),
            F.when(F.col("list_price").isNull(), F.lit("list_price não pôde ser convertido")).otherwise(F.lit(None)),
            F.when(
                F.col("currency") != "BRL", F.lit(F.concat(F.lit("currency != BRL: "), F.col("currency")))
            ).otherwise(F.lit(None)),
            F.when(F.col("updated_at").isNull(), F.lit("updated_at inválido")).otherwise(F.lit(None)),
        )
    ),
).withColumn("_dq_status", F.when(F.size("_dq_reasons") == 0, F.lit("clean")).otherwise(F.lit("warning")))

# COMMAND ----------

# 4. Schema final + escrita
df_final = df_with_dq.select(
    "product_code",
    "product_name",
    "category",
    "subcategory",
    "is_active",
    "list_price",
    "currency",
    "family",
    "tags",
    "updated_at",
    "_dq_status",
    "_dq_reasons",
    "_source_file",
    "_ingestion_timestamp",
)

(
    df_final.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_SCHEMA}.produtos")
)

print(
    f"\n[OK] workspace.silver.produtos gravada. Total: {spark.table(f'{SILVER_SCHEMA}.produtos').count()} linhas."
)
spark.table(f"{SILVER_SCHEMA}.produtos").groupBy("_dq_status").count().show()
spark.table(f"{SILVER_SCHEMA}.produtos").show(5, truncate=False)
