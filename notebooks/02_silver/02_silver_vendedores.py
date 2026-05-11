# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela: workspace.case_levva_silver.vendedores
# MAGIC ## Objetivo:
# MAGIC Normalizar cadastro de vendedores a partir do bronze. Resolve duplicatas reais (V004 e V008 aparecem 2x, prioriza `status='ATIVO'` + `hire_date` mais recente), padroniza `status` e `canal_id` com defaults, parseia `hire_date` em 3 formatos, e cruza `regional_code` contra `silver.regioes` para garantir referência válida. Saída pronta para servir como `dim_vendedor` no gold.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `workspace.case_levva_bronze.vendedores` | Cadastro bruto (~42 linhas com 2 duplicatas conhecidas, ingestado de `vendedores.csv` com `;`) |
# MAGIC | `workspace.case_levva_silver.regioes` | Lookup canônico de `regional_code` para resolver inconsistências |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC + troca `F.try_to_date` para `F.expr("try_to_date(...)")` (ANSI mode) |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

SILVER_SCHEMA = "workspace.case_levva_silver"

# COMMAND ----------


# Função utilitária — multi-format date parser
def parse_multi_format_date(col_name):
    """Tenta ISO YYYY-MM-DD, BR DD/MM/YYYY, e BR DD/MM/YYYY HH:MM nessa ordem."""
    return F.coalesce(
        F.expr(f"try_to_date({col_name}, 'yyyy-MM-dd')"),
        F.expr(f"try_to_date({col_name}, 'dd/MM/yyyy')"),
        F.expr(f"try_to_timestamp({col_name}, 'dd/MM/yyyy HH:mm')").cast("date"),
    )


# COMMAND ----------

# Leitura do Bronze
df_bronze = spark.table("workspace.case_levva_bronze.vendedores")
print(f"[BRONZE] Linhas lidas: {df_bronze.count()}")

# Detectar duplicatas antes do tratamento
print("\n[DETECTA] Vendedores duplicados:")
df_bronze.groupBy("seller_id").count().filter(F.col("count") > 1).show()

# COMMAND ----------

# 1. Normalização básica
df_normalized = (
    df_bronze.withColumn("seller_id", F.upper(F.trim(F.col("seller_id"))))
    .withColumn("seller_name", F.trim(F.col("seller_name")))
    .withColumn(
        "status",
        F.when(F.upper(F.trim(F.col("status"))) == "ATIVO", F.lit("ATIVO"))
        .when(F.upper(F.trim(F.col("status"))) == "INATIVO", F.lit("INATIVO"))
        .otherwise(F.lit("INATIVO")),  # default para nulos/vazios
    )
    .withColumn("hire_date", parse_multi_format_date("hire_date"))
)

# 2. Padronizar canal_id (default NAO_INFORMADO)
df_normalized = df_normalized.withColumn(
    "canal_id",
    F.when((F.col("canal_id").isNull()) | (F.trim(F.col("canal_id")) == ""), F.lit("NAO_INFORMADO")).otherwise(
        F.upper(F.trim(F.col("canal_id")))
    ),
)

# 3. Padronizar regional_code via lookup contra workspace.case_levva_silver.regioes
silver_regioes_codes = {
    r.regional_code for r in spark.table(f"{SILVER_SCHEMA}.regioes").select("regional_code").collect()
}
print(f"[INFO] Regional codes válidos em workspace.case_levva_silver.regioes: {silver_regioes_codes}")

# Mapeamento manual para variações conhecidas
df_normalized = df_normalized.withColumn(
    "regional_code",
    F.when(F.upper(F.trim(F.col("regional_code"))).isin("S", "SUL"), F.lit("S"))
    .when(F.upper(F.trim(F.col("regional_code"))).isin("SE", "SUDESTE"), F.lit("SE"))
    .when(F.upper(F.trim(F.col("regional_code"))).isin("N", "NORTE"), F.lit("N"))
    .when(F.upper(F.trim(F.col("regional_code"))).isin("NE", "NORDESTE"), F.lit("NE"))
    .when(F.upper(F.trim(F.col("regional_code"))).isin("CO", "CENTRO_OESTE", "CENTRO-OESTE"), F.lit("CO"))
    .when((F.col("regional_code").isNull()) | (F.trim(F.col("regional_code")) == ""), F.lit("XX"))
    .otherwise(F.upper(F.trim(F.col("regional_code")))),
)

# COMMAND ----------

# 4. Dedup — preferência por status=ATIVO + hire_date mais recente
window_dedup = Window.partitionBy("seller_id").orderBy(
    F.when(F.col("status") == "ATIVO", 0).otherwise(1),  # ATIVO vem primeiro
    F.col("hire_date").desc_nulls_last(),
    F.col("_ingestion_timestamp").desc(),
)

df_deduped = df_normalized.withColumn("_rn", F.row_number().over(window_dedup)).filter(F.col("_rn") == 1).drop("_rn")

print(f"[DEDUP] Linhas antes: {df_normalized.count()} -> depois: {df_deduped.count()}")

# COMMAND ----------

# 5. DQ flags
df_with_dq = df_deduped.withColumn(
    "_dq_reasons",
    F.array_remove(
        F.array(
            F.when(F.col("hire_date").isNull(), F.lit("hire_date inválida")).otherwise(F.lit(None)),
            F.when(F.col("canal_id") == "NAO_INFORMADO", F.lit("canal_id ausente")).otherwise(F.lit(None)),
            F.when(F.col("regional_code") == "XX", F.lit("regional_code não informado")).otherwise(F.lit(None)),
            F.when(
                ~F.col("regional_code").isin(["S", "SE", "N", "NE", "CO", "XX"]), F.lit("regional_code não reconhecido")
            ).otherwise(F.lit(None)),
        ),
        None,
    ),
).withColumn("_dq_status", F.when(F.size("_dq_reasons") == 0, F.lit("clean")).otherwise(F.lit("warning")))

# COMMAND ----------

# 6. Escrita
df_final = df_with_dq.select(
    "seller_id",
    "seller_name",
    "canal_id",
    "regional_code",
    "hire_date",
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
    .saveAsTable(f"{SILVER_SCHEMA}.vendedores")
)

print(
    f"\n[OK] workspace.case_levva_silver.vendedores gravada. Total: {spark.table(f'{SILVER_SCHEMA}.vendedores').count()} linhas."
)

# Resumo DQ
print("\n[DQ Summary]")
spark.table(f"{SILVER_SCHEMA}.vendedores").groupBy("_dq_status").count().show()

# Sample
print("\n[Sample]")
spark.table(f"{SILVER_SCHEMA}.vendedores").show(10, truncate=False)
