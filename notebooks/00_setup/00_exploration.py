# Databricks notebook source
# MAGIC %pip install openpyxl --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook: 00_exploration
# MAGIC ## Objetivo:
# MAGIC Profiling read-only das 9 fontes brutas disponibilizadas no Volume `workspace.landing.sources`. Para cada fonte: schema inferido, contagem de linhas, amostra, distribuição de nulls e valores distintos em colunas de baixa cardinalidade. **Não escreve nenhuma tabela.**
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `/Volumes/workspace/landing/sources/erp_pedidos_cabecalho_2025.csv` | CSV `;` com cabeçalho dos pedidos (~403 linhas) |
# MAGIC | `/Volumes/workspace/landing/sources/erp_pedidos_itens_2025.csv` | CSV `,` com itens dos pedidos (~995 linhas) |
# MAGIC | `/Volumes/workspace/landing/sources/vendedores.csv` | CSV `;` com cadastro de vendedores (~42 linhas) |
# MAGIC | `/Volumes/workspace/landing/sources/legado_regioes_pipe.txt` | Texto delimitado por `\|` com regiões (~9 linhas) |
# MAGIC | `/Volumes/workspace/landing/sources/cadastro_produtos_api_dump.json` | JSON aninhado com produtos (~65) |
# MAGIC | `/Volumes/workspace/landing/sources/logistica_entregas.json` | JSON array aninhado com entregas (~1700) |
# MAGIC | `/Volumes/workspace/landing/sources/atendimento_ocorrencias.ndjson` | NDJSON com tickets de ocorrência (~269) |
# MAGIC | `/Volumes/workspace/landing/sources/crm_clientes_export.xlsx` | XLSX com clientes (~183) |
# MAGIC | `/Volumes/workspace/landing/sources/comercial_canais.xlsx` | XLSX com canais comerciais (sheet `canais`, ~8) |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter para Unity Catalog Volume + ajustes ANSI mode (cast double->int) |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd

SOURCES_BASE = "/Volumes/workspace/landing/sources"

SOURCES = {
    "erp_pedidos_cabecalho_2025.csv": {"format": "csv", "sep": ";", "encoding": "UTF-8"},
    "erp_pedidos_itens_2025.csv": {"format": "csv", "sep": ",", "encoding": "UTF-8"},
    "vendedores.csv": {"format": "csv", "sep": ";", "encoding": "UTF-8"},
    "legado_regioes_pipe.txt": {"format": "csv", "sep": "|", "encoding": "UTF-8"},
    "cadastro_produtos_api_dump.json": {"format": "json", "multiline": True},
    "logistica_entregas.json": {"format": "json", "multiline": True},
    "atendimento_ocorrencias.ndjson": {"format": "json", "multiline": False},
    "crm_clientes_export.xlsx": {"format": "xlsx"},
    "comercial_canais.xlsx": {"format": "xlsx", "sheet_name": "canais"},
}

# COMMAND ----------

files_in_volume = dbutils.fs.ls(SOURCES_BASE)
print(f"[INFO] Arquivos em {SOURCES_BASE}:")
for f in files_in_volume:
    print(f"  - {f.name} ({f.size:,} bytes)")

files_present = {f.name for f in files_in_volume}
missing = set(SOURCES.keys()) - files_present
extra = files_present - set(SOURCES.keys())
if missing:
    print(f"\n[ERRO] Arquivos faltando: {missing}")
if extra:
    print(f"\n[INFO] Arquivos nao esperados (ignorados): {extra}")
assert not missing, f"Arquivos esperados ausentes: {missing}"
print(f"\n[OK] Todos os {len(SOURCES)} sources esperados estao presentes.")

# COMMAND ----------


def profile_dataframe(df, name: str, n_sample: int = 3):
    print(f"\n{'=' * 80}")
    print(f"[PROFILE] {name}")
    print(f"{'=' * 80}")
    print(f"\n--- Schema ({len(df.columns)} colunas) ---")
    df.printSchema()
    total = df.count()
    print(f"\n--- Total de linhas: {total:,} ---")
    print(f"\n--- Amostra ({n_sample} linhas) ---")
    df.show(n_sample, truncate=False, vertical=True)
    print(f"\n--- Null counts por coluna ---")
    null_counts = (
        df.select([F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]).collect()[0].asDict()
    )
    for c, n_nulls in null_counts.items():
        pct = (n_nulls / total * 100) if total > 0 else 0
        marker = " (>10%)" if pct > 10 else ""
        print(f"  {c}: {n_nulls:,} nulls ({pct:.1f}%){marker}")
    print(f"\n--- Distinct values em colunas de baixa cardinalidade ---")
    for c in df.columns:
        try:
            n_distinct = df.select(c).distinct().count()
            if 1 < n_distinct < 50:
                values = sorted([str(r[0]) for r in df.select(c).distinct().limit(50).collect() if r[0] is not None])
                print(f"  {c} ({n_distinct} distinct): {values}")
        except Exception as e:
            print(f"  {c}: erro ao calcular distinct ({e})")
    return total


# COMMAND ----------

df_pedidos_cab = (
    spark.read.option("sep", ";")
    .option("header", True)
    .option("encoding", "UTF-8")
    .csv(f"{SOURCES_BASE}/erp_pedidos_cabecalho_2025.csv")
)
profile_dataframe(df_pedidos_cab, "erp_pedidos_cabecalho_2025.csv")

# COMMAND ----------

print("[INVESTIGATE] Formatos distintos em order_date:")
df_pedidos_cab.select("order_date").distinct().show(20, truncate=False)
print("[INVESTIGATE] Formatos distintos em status_order:")
df_pedidos_cab.select("status_order").groupBy("status_order").count().orderBy(F.desc("count")).show(truncate=False)
print("[INVESTIGATE] Sample de payment_details (JSON aninhado):")
df_pedidos_cab.select("payment_details").show(5, truncate=False)

# COMMAND ----------

df_pedidos_itens = (
    spark.read.option("sep", ",")
    .option("header", True)
    .option("encoding", "UTF-8")
    .csv(f"{SOURCES_BASE}/erp_pedidos_itens_2025.csv")
)
profile_dataframe(df_pedidos_itens, "erp_pedidos_itens_2025.csv")

# COMMAND ----------

print("[INVESTIGATE] Consistencia de total_item vs quantity * unit_price:")
df_check = (
    df_pedidos_itens.select(
        F.col("order_id"),
        F.col("item_seq"),
        F.col("quantity").cast("double").cast("int").alias("qty"),
        F.regexp_replace(F.col("unit_price"), ",", ".").cast("double").alias("up"),
        F.regexp_replace(F.col("total_item"), ",", ".").cast("double").alias("ti"),
    )
    .withColumn("recalc", F.col("qty") * F.col("up"))
    .withColumn("diff", F.abs(F.col("ti") - F.col("recalc")))
)
df_check.filter(F.col("diff") > 0.01).show(20, truncate=False)
print(f"Linhas com diferenca > 0.01: {df_check.filter(F.col('diff') > 0.01).count()}")

# COMMAND ----------

df_vendedores = (
    spark.read.option("sep", ";")
    .option("header", True)
    .option("encoding", "UTF-8")
    .csv(f"{SOURCES_BASE}/vendedores.csv")
)
profile_dataframe(df_vendedores, "vendedores.csv")

# COMMAND ----------

print("[INVESTIGATE] Vendedores duplicados (seller_id):")
df_vendedores.groupBy("seller_id").count().filter(F.col("count") > 1).show(truncate=False)

# COMMAND ----------

df_regioes = (
    spark.read.option("sep", "|")
    .option("header", True)
    .option("encoding", "UTF-8")
    .csv(f"{SOURCES_BASE}/legado_regioes_pipe.txt")
)
profile_dataframe(df_regioes, "legado_regioes_pipe.txt")

# COMMAND ----------

print("[INVESTIGATE] Todos os regional_code:")
df_regioes.select("regional_code", "regional_name").show(truncate=False)

# COMMAND ----------

df_produtos = spark.read.option("multiline", True).json(f"{SOURCES_BASE}/cadastro_produtos_api_dump.json")
profile_dataframe(df_produtos, "cadastro_produtos_api_dump.json")

# COMMAND ----------

df_entregas = spark.read.option("multiline", True).json(f"{SOURCES_BASE}/logistica_entregas.json")
profile_dataframe(df_entregas, "logistica_entregas.json")

# COMMAND ----------

print("[INVESTIGATE] Sample de entregas (estrutura aninhada):")
df_entregas.select("delivery_id", "carrier", "timestamps", "destination").show(3, truncate=False, vertical=True)

# COMMAND ----------

df_ocorrencias = spark.read.json(f"{SOURCES_BASE}/atendimento_ocorrencias.ndjson")
profile_dataframe(df_ocorrencias, "atendimento_ocorrencias.ndjson")

# COMMAND ----------

print("[INVESTIGATE] event_type:")
df_ocorrencias.groupBy("event_type").count().orderBy(F.desc("count")).show(truncate=False)
print("[INVESTIGATE] severity:")
df_ocorrencias.groupBy("severity").count().orderBy(F.desc("count")).show(truncate=False)
print("[INVESTIGATE] status:")
df_ocorrencias.groupBy("status").count().orderBy(F.desc("count")).show(truncate=False)

# COMMAND ----------

local_path_clientes = "/Volumes/workspace/landing/sources/crm_clientes_export.xlsx"
df_clientes_pd = pd.read_excel(local_path_clientes)
print(f"[INFO] crm_clientes_export.xlsx - shape: {df_clientes_pd.shape}")
print(f"\n[INFO] Colunas: {list(df_clientes_pd.columns)}")
print(f"\n[INFO] Sample:")
print(df_clientes_pd.head(5).to_string())
print(f"\n[INFO] dtypes:")
print(df_clientes_pd.dtypes)
print(f"\n[INFO] Null counts:")
print(df_clientes_pd.isnull().sum())

df_clientes = spark.createDataFrame(df_clientes_pd)
profile_dataframe(df_clientes, "crm_clientes_export.xlsx (Spark DF)")

# COMMAND ----------

local_path_canais = "/Volumes/workspace/landing/sources/comercial_canais.xlsx"
df_canais_pd = pd.read_excel(local_path_canais, sheet_name="canais")
print(f"[INFO] comercial_canais.xlsx - shape: {df_canais_pd.shape}")
print(f"\n[INFO] Colunas: {list(df_canais_pd.columns)}")
print(f"\n[INFO] Sample:")
print(df_canais_pd.head(20).to_string())
print(f"\n[INFO] dtypes:")
print(df_canais_pd.dtypes)

df_canais = spark.createDataFrame(df_canais_pd)
profile_dataframe(df_canais, "comercial_canais.xlsx (Spark DF)")

# COMMAND ----------

print("[INVESTIGATE] Customer codes em pedidos vs clientes:")
print(f"  Pedidos: {df_pedidos_cab.select('customer_code').distinct().count()} distintos")
print(f"  Clientes: colunas = {list(df_clientes_pd.columns)}")

print(f"\n[INVESTIGATE] Seller IDs:")
seller_ids_pedidos = df_pedidos_cab.select("seller_id").distinct()
seller_ids_vendedores = df_vendedores.select("seller_id").distinct()
print(f"  Pedidos: {seller_ids_pedidos.count()} distintos")
print(f"  Vendedores: {seller_ids_vendedores.count()} distintos")
orphans = seller_ids_pedidos.exceptAll(seller_ids_vendedores)
print(f"  Orfaos (pedidos sem vendedor cadastrado): {orphans.count()}")
if orphans.count() > 0:
    orphans.show(20)

# COMMAND ----------

print("\n[INVESTIGATE] Cobertura de order_id em fatos:")
order_ids_pedidos = df_pedidos_cab.select("order_id").distinct()
order_ids_itens = df_pedidos_itens.select("order_id").distinct()
order_ids_entregas = df_entregas.select(F.col("order_ref").alias("order_id")).distinct()
order_ids_ocorrencias = df_ocorrencias.select("order_id").distinct()
print(f"  Pedidos: {order_ids_pedidos.count()} distintos")
print(f"  Itens (order_id distintos): {order_ids_itens.count()}")
print(f"  Entregas (order_ref distintos): {order_ids_entregas.count()}")
print(f"  Ocorrencias (order_id distintos): {order_ids_ocorrencias.count()}")
print(f"\n  Pedidos SEM itens: {order_ids_pedidos.exceptAll(order_ids_itens).count()}")
print(f"  Itens com order_id ORFAO: {order_ids_itens.exceptAll(order_ids_pedidos).count()}")

# COMMAND ----------

print("\n[INVESTIGATE] Product codes:")
product_codes_itens = df_pedidos_itens.select("product_code").distinct()
product_codes_produtos = df_produtos.select(F.col("product.product_id").alias("product_code")).distinct()
print(f"  Itens (product_code distintos): {product_codes_itens.count()}")
print(f"  Produtos cadastrados: {product_codes_produtos.count()}")
print(f"  Produtos vendidos sem cadastro: {product_codes_itens.exceptAll(product_codes_produtos).count()}")
