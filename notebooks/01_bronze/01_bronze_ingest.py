# Databricks notebook source
# MAGIC %pip install openpyxl --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: 01_bronze_ingest
# MAGIC ## Objetivo:
# MAGIC Ingestão multi-formato das 9 fontes brutas em Delta, preservando o dado original como string e adicionando metadata técnica (`_source_file`, `_ingestion_timestamp`, `_record_id`). Tabelas materializadas no schema `workspace.bronze`. Modo `overwrite` garante idempotência: reprocessamento não corrompe estado.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `/Volumes/workspace/landing/sources/erp_pedidos_cabecalho_2025.csv` | CSV `;` -> `workspace.bronze.pedidos_cabecalho` |
# MAGIC | `/Volumes/workspace/landing/sources/erp_pedidos_itens_2025.csv` | CSV `,` -> `workspace.bronze.pedidos_itens` |
# MAGIC | `/Volumes/workspace/landing/sources/vendedores.csv` | CSV `;` -> `workspace.bronze.vendedores` |
# MAGIC | `/Volumes/workspace/landing/sources/legado_regioes_pipe.txt` | Texto pipe `\|` -> `workspace.bronze.regioes` |
# MAGIC | `/Volumes/workspace/landing/sources/cadastro_produtos_api_dump.json` | JSON aninhado -> `workspace.bronze.produtos` (structs serializados como JSON string) |
# MAGIC | `/Volumes/workspace/landing/sources/logistica_entregas.json` | JSON array aninhado -> `workspace.bronze.entregas` (structs serializados como JSON string) |
# MAGIC | `/Volumes/workspace/landing/sources/atendimento_ocorrencias.ndjson` | NDJSON -> `workspace.bronze.ocorrencias` |
# MAGIC | `/Volumes/workspace/landing/sources/crm_clientes_export.xlsx` | XLSX via pandas -> `workspace.bronze.clientes` |
# MAGIC | `/Volumes/workspace/landing/sources/comercial_canais.xlsx` | XLSX via pandas (sheet `canais`) -> `workspace.bronze.canais` |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC: paths Volume + schema namespace `workspace.bronze` |
# MAGIC | 2026-05-10 | Wilson Lucas | `sheet_name="canais"` explícito; lista determinística de tabelas no resumo final |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, IntegerType, DoubleType, BooleanType
import pandas as pd

SOURCES_BASE = "/Volumes/workspace/landing/sources"
SOURCES_LOCAL = "/Volumes/workspace/landing/sources"
BRONZE_SCHEMA = "workspace.bronze"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
print(f"[OK] Schema {BRONZE_SCHEMA} pronto.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função utilitária — adicionar metadata e gravar

# COMMAND ----------


def write_bronze(df, table_name: str, source_file: str):
    """
    Adiciona colunas técnicas e grava em Delta no schema workspace.bronze.

    Args:
    df: DataFrame Spark com colunas como string
    table_name: nome curto da tabela (sem prefixo de schema)
    source_file: nome do arquivo original (para rastreabilidade)
    """
    # Cast tudo pra string (preserva formato original)
    cols_as_string = [F.col(c).cast(StringType()).alias(c) for c in df.columns]
    df_string = df.select(cols_as_string)

    # Adiciona metadata
    df_with_meta = (
        df_string.withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_record_id", F.monotonically_increasing_id())
    )

    # Persiste em Delta com overwrite (idempotente)
    full_table = f"{BRONZE_SCHEMA}.{table_name}"
    (df_with_meta.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table))

    count = spark.table(full_table).count()
    print(f"[OK] {full_table}: {count:,} linhas | colunas={len(df_with_meta.columns)}")

    return df_with_meta


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pedidos — Cabeçalho

# COMMAND ----------

df_raw = (
    spark.read.option("sep", ";")
    .option("header", True)
    .option("encoding", "UTF-8")
    .csv(f"{SOURCES_BASE}/erp_pedidos_cabecalho_2025.csv")
)

write_bronze(df_raw, "pedidos_cabecalho", "erp_pedidos_cabecalho_2025.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pedidos — Itens

# COMMAND ----------

df_raw = (
    spark.read.option("sep", ",")
    .option("header", True)
    .option("encoding", "UTF-8")
    .csv(f"{SOURCES_BASE}/erp_pedidos_itens_2025.csv")
)

write_bronze(df_raw, "pedidos_itens", "erp_pedidos_itens_2025.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Vendedores

# COMMAND ----------

df_raw = (
    spark.read.option("sep", ";")
    .option("header", True)
    .option("encoding", "UTF-8")
    .csv(f"{SOURCES_BASE}/vendedores.csv")
)

write_bronze(df_raw, "vendedores", "vendedores.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Regiões (legado pipe-delimited)

# COMMAND ----------

df_raw = (
    spark.read.option("sep", "|")
    .option("header", True)
    .option("encoding", "UTF-8")
    .csv(f"{SOURCES_BASE}/legado_regioes_pipe.txt")
)

write_bronze(df_raw, "regioes", "legado_regioes_pipe.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Produtos (JSON aninhado, multiline)

# COMMAND ----------

# Para JSON aninhado, mantemos a estrutura original — o flatten acontece no Silver
df_raw = spark.read.option("multiline", True).json(f"{SOURCES_BASE}/cadastro_produtos_api_dump.json")

# Estrutura aninhada — converter struct para string JSON para Bronze (preserva tudo como string)
df_flat_json = df_raw.select(
    F.col("updated_at").cast(StringType()).alias("updated_at"),
    F.to_json(F.col("product")).alias("product_json"),
    F.to_json(F.col("pricing")).alias("pricing_json"),
    F.to_json(F.col("attributes")).alias("attributes_json"),
)

write_bronze(df_flat_json, "produtos", "cadastro_produtos_api_dump.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Entregas (JSON array, multiline, nested)

# COMMAND ----------

df_raw = spark.read.option("multiline", True).json(f"{SOURCES_BASE}/logistica_entregas.json")

# Aplanar structs aninhados como JSON string no Bronze
df_flat_json = df_raw.select(
    F.col("delivery_id").cast(StringType()).alias("delivery_id"),
    F.col("order_ref").cast(StringType()).alias("order_ref"),
    F.col("delivery_status").cast(StringType()).alias("delivery_status"),
    F.col("cost").cast(StringType()).alias("cost"),
    F.to_json(F.col("carrier")).alias("carrier_json"),
    F.to_json(F.col("timestamps")).alias("timestamps_json"),
    F.to_json(F.col("destination")).alias("destination_json"),
)

write_bronze(df_flat_json, "entregas", "logistica_entregas.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Ocorrências (NDJSON — 1 obj por linha)

# COMMAND ----------

df_raw = spark.read.json(f"{SOURCES_BASE}/atendimento_ocorrencias.ndjson")  # multiline=False (default) para NDJSON

write_bronze(df_raw, "ocorrencias", "atendimento_ocorrencias.ndjson")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Clientes (XLSX via pandas)

# COMMAND ----------

# Spark não lê XLSX nativamente. Estratégia: pandas -> Spark DataFrame.
df_pd = pd.read_excel(f"{SOURCES_LOCAL}/crm_clientes_export.xlsx")

# Converte tudo pra string (consistente com princípio Bronze)
df_pd = df_pd.astype(str).replace({"nan": None})  # pandas usa "nan" como string para NaN

df_raw = spark.createDataFrame(df_pd)

write_bronze(df_raw, "clientes", "crm_clientes_export.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Canais (XLSX via pandas)
# MAGIC
# MAGIC O arquivo tem **uma sheet só chamada `canais`** (não `Sheet1`). Explícito por robustez — se a planilha ganhar outras sheets no futuro, não pegamos a errada.

# COMMAND ----------

df_pd = pd.read_excel(f"{SOURCES_LOCAL}/comercial_canais.xlsx", sheet_name="canais")
df_pd = df_pd.astype(str).replace({"nan": None})

df_raw = spark.createDataFrame(df_pd)

write_bronze(df_raw, "canais", "comercial_canais.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação final — listar tabelas Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.bronze;

# COMMAND ----------

# Contar linhas em cada Bronze
# Lista determinística (evita SHOW TABLES que devolve `_sqldf` temp view do magic SQL anterior)
print("\n[BRONZE] Resumo de ingestão:")
print(f"{'Tabela':<35} {'Linhas':>12} {'Source':<40}")
print("-" * 90)

EXPECTED_TABLES = [
    "pedidos_cabecalho",
    "pedidos_itens",
    "produtos",
    "clientes",
    "canais",
    "vendedores",
    "regioes",
    "ocorrencias",
    "entregas",
]
for t in EXPECTED_TABLES:
    df = spark.table(f"{BRONZE_SCHEMA}.{t}")
    count = df.count()
    source = df.select(F.first("_source_file")).first()[0]
    print(f"{BRONZE_SCHEMA + '.' + t:<35} {count:>12,} {source:<40}")

print("\n[OK] Bronze layer completo.")
