# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela: workspace.case_levva_silver.clientes
# MAGIC ## Objetivo:
# MAGIC Normalizar o cadastro de clientes a partir do bronze. Trata as 11 issues mapeadas em `00_exploration` no XLSX original (183 linhas, sheet `Sheet1`): dedup por `customer_id` (3 duplicatas), parse de `data_cadastro` em 3 formatos misturados, mapeamento exaustivo de `estado` (UF + nome + typo) para UF code de 2 letras, validação de `email` via regex, e padronização de enums (`porte`, `segmento`, `status_cliente`). Saída pronta para servir como `dim_cliente` no gold.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `workspace.case_levva_bronze.clientes` | Cadastro bruto de clientes (183 linhas, ingestado de `crm_clientes_export.xlsx` via pandas) |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook (esqueleto) |
# MAGIC | 2026-05-10 | Wilson Lucas | Schema real capturado: UF_MAP exaustivo (18 variantes), parse de 3 formatos de data, validação email, dedup por `updated_at` |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC: schema `workspace.case_levva_silver`; troca de `F.to_date` por `F.expr("try_to_date(...)")` para ANSI mode |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

SILVER_SCHEMA = "workspace.case_levva_silver"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")

# COMMAND ----------

df_bronze = spark.table("workspace.case_levva_bronze.clientes")
print(f"[BRONZE] Linhas: {df_bronze.count()} | Colunas: {df_bronze.columns}")
df_bronze.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lookup de estado -> UF
# MAGIC
# MAGIC O campo `estado` chega com 3 padrões distintos (nome completo lower, nome completo title, UF de 2 letras) e ao menos 1 typo (`Sta Catarina`). Em vez de regex, uso um mapa exaustivo: tudo que não casa fica `null` e cai como warning de DQ — assim a tabela continua usável e a issue fica visível.

# COMMAND ----------

UF_MAP = {
    # UF code já correto
    "AC": "AC",
    "AL": "AL",
    "AP": "AP",
    "AM": "AM",
    "BA": "BA",
    "CE": "CE",
    "DF": "DF",
    "ES": "ES",
    "GO": "GO",
    "MA": "MA",
    "MT": "MT",
    "MS": "MS",
    "MG": "MG",
    "PA": "PA",
    "PB": "PB",
    "PR": "PR",
    "PE": "PE",
    "PI": "PI",
    "RJ": "RJ",
    "RN": "RN",
    "RS": "RS",
    "RO": "RO",
    "RR": "RR",
    "SC": "SC",
    "SP": "SP",
    "SE": "SE",
    "TO": "TO",
    # Nomes que aparecem nos dados (lower-case após UPPER+trim)
    "PARANA": "PR",
    "PARANÁ": "PR",
    "MINAS GERAIS": "MG",
    "SAO PAULO": "SP",
    "SÃO PAULO": "SP",
    "RIO DE JANEIRO": "RJ",
    "SANTA CATARINA": "SC",
    "STA CATARINA": "SC",  # typo observado
    "RIO GRANDE DO SUL": "RS",
    "BAHIA": "BA",
    "GOIAS": "GO",
    "GOIÁS": "GO",
    "PERNAMBUCO": "PE",
    "CEARA": "CE",
    "CEARÁ": "CE",
}

# Constrói expression `CASE WHEN ... END` para o mapping
uf_when = F.col("estado_norm")
for k, v in UF_MAP.items():
    pass  # não usado; construo via create_map abaixo

uf_map_expr = F.create_map(*[F.lit(x) for kv in UF_MAP.items() for x in kv])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Normalização campo a campo

# COMMAND ----------

DATE_FORMATS = ["yyyy-MM-dd", "yyyy/MM/dd", "dd/MM/yyyy"]

df_norm = (
    df_bronze
    # IDs e textos
    .withColumn("customer_id", F.upper(F.trim(F.col("customer_id"))))
    .withColumn("nome_cliente", F.trim(F.col("nome_cliente")))
    .withColumn("segmento", F.upper(F.trim(F.col("segmento"))))
    .withColumn("porte", F.upper(F.trim(F.col("porte"))))
    .withColumn("cidade", F.initcap(F.trim(F.col("cidade"))))
    .withColumn("status_cliente", F.upper(F.trim(F.col("status_cliente"))))
    # Estado -> UF via lookup; quem não casa fica null
    .withColumn("estado_norm", F.upper(F.trim(F.col("estado"))))
    .withColumn("uf", uf_map_expr[F.col("estado_norm")])
    # Data cadastro: tenta 3 formatos em ordem
    .withColumn(
        "data_cadastro_parsed",
        F.coalesce(*[F.expr(f"try_to_date(data_cadastro, '{fmt}')") for fmt in DATE_FORMATS]),
    )
    # Updated_at: timestamp permissivo (Spark try_to_timestamp aceita ISO + variações comuns)
    .withColumn("updated_at_parsed", F.expr("try_to_timestamp(updated_at)"))
    # Email: regex bem permissivo só pra detectar ausência de @ ou .
    .withColumn(
        "email_valid",
        F.col("email").isNotNull() & F.col("email").rlike(r".+@.+\..+"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dedup por customer_id
# MAGIC
# MAGIC `customer_id` é PK lógica. 3 linhas a mais que IDs únicos sinalizam atualização cadastral; mantemos o registro com `updated_at` mais recente. Em empate, desempata por `_record_id` (ordem de ingestão).

# COMMAND ----------

window_dedup = Window.partitionBy("customer_id").orderBy(
    F.col("updated_at_parsed").desc_nulls_last(),
    F.col("_record_id").desc(),
)

df_dedup = df_norm.withColumn("_rn", F.row_number().over(window_dedup)).filter(F.col("_rn") == 1).drop("_rn")

print(f"[DEDUP] {df_norm.count()} -> {df_dedup.count()} (esperado: 183 -> 180)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. DQ flags

# COMMAND ----------

df_dq = df_dedup.withColumn(
    "_dq_reasons",
    F.array_compact(
        F.array(
            F.when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), F.lit("customer_id ausente")),
            F.when(~F.col("email_valid"), F.lit("email invalido ou ausente")),
            F.when(F.col("segmento").isNull(), F.lit("segmento ausente")),
            F.when(F.col("porte").isNull(), F.lit("porte ausente")),
            F.when(F.col("status_cliente").isNull(), F.lit("status_cliente ausente")),
            F.when(F.col("uf").isNull(), F.lit("estado nao mapeado para UF")),
            F.when(F.col("data_cadastro_parsed").isNull(), F.lit("data_cadastro nao parseavel")),
        )
    ),
).withColumn(
    "_dq_status",
    F.when(
        F.array_contains(F.col("_dq_reasons"), "customer_id ausente")
        | F.array_contains(F.col("_dq_reasons"), "email invalido ou ausente"),
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
    F.col("customer_id"),
    F.col("nome_cliente"),
    F.col("segmento"),
    F.col("porte"),
    F.col("cidade"),
    F.col("uf").alias("uf"),
    F.col("estado").alias("estado_original"),  # mantém para auditoria
    F.col("data_cadastro_parsed").alias("data_cadastro"),
    F.col("email"),
    F.col("email_valid"),
    F.col("status_cliente"),
    F.col("updated_at_parsed").alias("updated_at"),
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
    .saveAsTable(f"{SILVER_SCHEMA}.clientes")
)

print(f"\n[OK] {SILVER_SCHEMA}.clientes gravada: {spark.table(f'{SILVER_SCHEMA}.clientes').count()} linhas.")

# Resumo DQ
print("\n[DQ] Distribuição de status:")
spark.table(f"{SILVER_SCHEMA}.clientes").groupBy("_dq_status").count().orderBy("_dq_status").show()

print("\n[DQ] Top razões warning/rejected:")
(
    spark.table(f"{SILVER_SCHEMA}.clientes")
    .filter(F.col("_dq_status") != "clean")
    .select(F.explode("_dq_reasons").alias("reason"))
    .groupBy("reason")
    .count()
    .orderBy(F.desc("count"))
    .show(truncate=False)
)

spark.table(f"{SILVER_SCHEMA}.clientes").show(5, truncate=False)
