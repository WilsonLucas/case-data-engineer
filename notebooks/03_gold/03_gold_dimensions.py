# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: 03_gold_dimensions — 6 dimensões
# MAGIC ## Objetivo:
# MAGIC Criar a camada de dimensões do star schema analítico (SCD Type 1) a partir das tabelas silver. Gera `dim_cliente`, `dim_produto`, `dim_canal`, `dim_regiao`, `dim_vendedor` (entidades) e `dim_data` (gerada cobrindo o range temporal dos fatos, com atributos derivados: ano, mês, trimestre, dia da semana, fim de semana). Saída consumida por `fact_*` e por `vw_kpi_business`.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `workspace.silver.clientes` | -> `workspace.gold.dim_cliente` |
# MAGIC | `workspace.silver.produtos` | -> `workspace.gold.dim_produto` |
# MAGIC | `workspace.silver.canais` | -> `workspace.gold.dim_canal` |
# MAGIC | `workspace.silver.regioes` | -> `workspace.gold.dim_regiao` |
# MAGIC | `workspace.silver.vendedores` | -> `workspace.gold.dim_vendedor` |
# MAGIC | `workspace.silver.pedidos_cabecalho` + `entregas` + `ocorrencias` | Usadas para inferir o range temporal de `dim_data` |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC: schema `workspace.gold` |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

GOLD_SCHEMA = "workspace.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_cliente

# COMMAND ----------

df_silver_clientes = spark.table("workspace.silver.clientes")
print(f"[INFO] workspace.silver.clientes columns: {df_silver_clientes.columns}")

# Schema mínimo (ajustar com colunas reais conforme schema do XLSX)
df_dim_cliente = (
    df_silver_clientes.select(
        F.col("customer_code"),
        *[
            c
            for c in df_silver_clientes.columns
            if c
            not in ("customer_code", "_dq_status", "_dq_reasons", "_source_file", "_ingestion_timestamp", "_record_id")
        ],
    )
    .filter(F.col("customer_code").isNotNull())
    .dropDuplicates(["customer_code"])
)

(
    df_dim_cliente.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_cliente")
)
print(f"[OK] {GOLD_SCHEMA}.dim_cliente: {spark.table(f'{GOLD_SCHEMA}.dim_cliente').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_cliente_history (SCD Type 2 demonstrativa - ADR-002)
# MAGIC Tabela paralela com versionamento historico. Hash MD5 sobre 4 colunas tracking
# MAGIC (segmento, estado, cidade, status). MERGE pattern para ingestoes subsequentes.

# COMMAND ----------

from delta.tables import DeltaTable
from datetime import datetime as _dt

# Snapshot atual com hash + atributos SCD2
def _coalesce_str(col):
    return F.coalesce(F.col(col).cast("string"), F.lit(""))

silver_clean = df_silver_clientes.filter(F.col("_dq_status") != "rejected").select(
    "customer_code",
    "segmento",
    "uf",
    "cidade",
    "status_cliente",
    F.col("_ingestion_timestamp").alias("ts_ingestao"),
)

scd2_snapshot = (
    silver_clean.withColumn(
        "scd_hash",
        F.md5(
            F.concat_ws(
                "||",
                _coalesce_str("segmento"),
                _coalesce_str("uf"),
                _coalesce_str("cidade"),
                _coalesce_str("status_cliente"),
            )
        ),
    )
    .withColumn("effective_date", F.to_date("ts_ingestao"))
    .withColumn("end_date", F.lit("9999-12-31").cast("date"))
    .withColumn("is_current", F.lit(True))
    .drop("ts_ingestao")
    .dropDuplicates(["customer_code"])
)

target_table = f"{GOLD_SCHEMA}.dim_cliente_history"

if not spark.catalog.tableExists(target_table):
    # Primeira ingestao: cria tabela do zero
    (scd2_snapshot.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table))
    print(f"[OK] {target_table}: criado com {spark.table(target_table).count()} linhas (todos is_current=true)")
else:
    # Ingestoes subsequentes: detecta mudancas via hash e aplica MERGE SCD2
    target = DeltaTable.forName(spark, target_table)
    current_active = target.toDF().filter(F.col("is_current") == True).select(
        F.col("customer_code").alias("cur_code"),
        F.col("scd_hash").alias("cur_hash"),
    )
    changes = (
        scd2_snapshot.alias("new")
        .join(current_active.alias("cur"), F.col("new.customer_code") == F.col("cur.cur_code"), "left")
        .filter(F.col("cur.cur_hash").isNull() | (F.col("new.scd_hash") != F.col("cur.cur_hash")))
        .select("new.*")
    )

    if changes.count() > 0:
        # 1) Fecha versoes ativas que mudaram
        (
            target.alias("t")
            .merge(
                changes.select("customer_code").alias("c"),
                "t.customer_code = c.customer_code AND t.is_current = true",
            )
            .whenMatchedUpdate(set={"is_current": F.lit(False), "end_date": F.current_date()})
            .execute()
        )
        # 2) Insere novas versoes
        changes.write.format("delta").mode("append").saveAsTable(target_table)
        print(f"[OK] {target_table}: {changes.count()} mudancas aplicadas via SCD2 MERGE")
    else:
        print(f"[OK] {target_table}: nenhuma mudanca detectada (idempotente)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_produto

# COMMAND ----------

df_silver_produtos = spark.table("workspace.silver.produtos")

df_dim_produto = (
    df_silver_produtos.select(
        F.col("product_code"),
        F.col("product_name"),
        F.col("category"),
        F.col("subcategory"),
        F.col("is_active"),
        F.col("list_price"),
        F.col("currency"),
        F.col("family"),
        F.col("tags"),
        F.col("updated_at"),
    )
    .filter(F.col("product_code").isNotNull())
    .dropDuplicates(["product_code"])
)

(
    df_dim_produto.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_produto")
)
print(f"[OK] {GOLD_SCHEMA}.dim_produto: {spark.table(f'{GOLD_SCHEMA}.dim_produto').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_canal

# COMMAND ----------

df_silver_canais = spark.table("workspace.silver.canais")

df_dim_canal = (
    df_silver_canais.select(
        F.col("canal_id"),
        *[
            c
            for c in df_silver_canais.columns
            if c not in ("canal_id", "_dq_status", "_dq_reasons", "_source_file", "_ingestion_timestamp", "_record_id")
        ],
    )
    .filter(F.col("canal_id").isNotNull())
    .dropDuplicates(["canal_id"])
)

(
    df_dim_canal.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_canal")
)
print(f"[OK] {GOLD_SCHEMA}.dim_canal: {spark.table(f'{GOLD_SCHEMA}.dim_canal').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_regiao

# COMMAND ----------

df_silver_regioes = spark.table("workspace.silver.regioes")

df_dim_regiao = df_silver_regioes.select(
    F.col("regional_code"),
    F.col("regional_name").alias("regiao_nome"),
    F.col("state").alias("estado"),
    F.col("manager_name").alias("gestor_nome"),
    F.col("active_flag"),
)

(
    df_dim_regiao.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_regiao")
)
print(f"[OK] {GOLD_SCHEMA}.dim_regiao: {spark.table(f'{GOLD_SCHEMA}.dim_regiao').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_vendedor

# COMMAND ----------

df_silver_vendedores = spark.table("workspace.silver.vendedores")

df_dim_vendedor = df_silver_vendedores.select(
    F.col("seller_id"),
    F.col("seller_name"),
    F.col("canal_id"),
    F.col("regional_code"),
    F.col("hire_date"),
    F.col("status"),
)

(
    df_dim_vendedor.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_vendedor")
)
print(f"[OK] {GOLD_SCHEMA}.dim_vendedor: {spark.table(f'{GOLD_SCHEMA}.dim_vendedor').count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_data
# MAGIC
# MAGIC Gerada cobrindo o range completo das datas em fatos (pedidos + entregas + ocorrências) com folga de ±30 dias.

# COMMAND ----------

# Determinar range de datas a partir das fontes
date_ranges = spark.sql("""
    SELECT
        LEAST(
            MIN(p.order_date),
            MIN(CAST(e.shipped_at AS DATE)),
            MIN(CAST(o.created_at AS DATE))
        ) AS min_date,
        GREATEST(
            MAX(p.order_date),
            MAX(p.promised_date),
            MAX(CAST(e.delivered_at AS DATE)),
            MAX(CAST(o.created_at AS DATE))
        ) AS max_date
    FROM workspace.silver.pedidos_cabecalho p
    LEFT JOIN workspace.silver.entregas e ON p.order_id = e.order_id
    LEFT JOIN workspace.silver.ocorrencias o ON p.order_id = o.order_id
""").collect()[0]

min_date = date_ranges.min_date
max_date = date_ranges.max_date

print(f"[INFO] Range detectado: {min_date} -> {max_date}")

# Feriados nacionais BR 2025 (12 oficiais, exclui Carnaval que e ponto facultativo)
FERIADOS_BR_2025 = {
    "2025-01-01": "Confraternizacao Universal",
    "2025-04-18": "Sexta-feira Santa",
    "2025-04-21": "Tiradentes",
    "2025-05-01": "Dia do Trabalho",
    "2025-06-19": "Corpus Christi",
    "2025-09-07": "Independencia do Brasil",
    "2025-10-12": "Nossa Senhora Aparecida",
    "2025-11-02": "Finados",
    "2025-11-15": "Proclamacao da Republica",
    "2025-11-20": "Dia da Consciencia Negra",
    "2025-12-25": "Natal",
    "2025-04-22": "Tiradentes (extra)",  # ajuste se necessario
}

# Gera dim_data com folga de 30 dias antes e depois (enriquecida com feriados BR)
df_dim_data = (
    spark.sql(f"""
        SELECT sequence(
            date_sub(to_date('{min_date}'), 30),
            date_add(to_date('{max_date}'), 30),
            interval 1 day
        ) AS dates
    """)
    .select(F.explode("dates").alias("data_completa"))
    .withColumn("data_id", F.date_format("data_completa", "yyyyMMdd").cast("int"))
    .withColumn("ano", F.year("data_completa"))
    .withColumn("mes", F.month("data_completa"))
    .withColumn("dia", F.dayofmonth("data_completa"))
    .withColumn("trimestre", F.quarter("data_completa"))
    .withColumn("mes_nome", F.date_format("data_completa", "MMMM"))
    .withColumn("dia_semana_num", F.dayofweek("data_completa"))
    .withColumn(
        "dia_semana",
        F.when(F.col("dia_semana_num") == 1, "Domingo")
        .when(F.col("dia_semana_num") == 2, "Segunda")
        .when(F.col("dia_semana_num") == 3, "Terça")
        .when(F.col("dia_semana_num") == 4, "Quarta")
        .when(F.col("dia_semana_num") == 5, "Quinta")
        .when(F.col("dia_semana_num") == 6, "Sexta")
        .when(F.col("dia_semana_num") == 7, "Sábado"),
    )
    .withColumn("fim_de_semana", F.col("dia_semana_num").isin(1, 7))
    .withColumn("ano_mes", F.date_format("data_completa", "yyyy-MM"))
    .withColumn("semana_iso", F.weekofyear("data_completa"))
    .withColumn("ano_semana_iso", F.concat(F.col("ano"), F.lit("-W"), F.lpad(F.col("semana_iso").cast("string"), 2, "0")))
    .withColumn("ultimo_dia_mes", F.col("data_completa") == F.last_day("data_completa"))
)

# Cria DataFrame de feriados e enriquece dim_data via left join
feriados_rows = [(d, n) for d, n in FERIADOS_BR_2025.items()]
df_feriados = spark.createDataFrame(feriados_rows, ["data_str", "holiday_name"]).withColumn(
    "data_completa", F.to_date("data_str", "yyyy-MM-dd")
)

df_dim_data = (
    df_dim_data.join(df_feriados.select("data_completa", "holiday_name"), on="data_completa", how="left")
    .withColumn("eh_feriado", F.col("holiday_name").isNotNull())
    .withColumn("eh_dia_util", ~F.col("fim_de_semana") & ~F.col("eh_feriado"))
    .select(
        "data_id",
        "data_completa",
        "ano",
        "mes",
        "dia",
        "trimestre",
        "mes_nome",
        "dia_semana",
        "dia_semana_num",
        "fim_de_semana",
        "ano_mes",
        "semana_iso",
        "ano_semana_iso",
        "ultimo_dia_mes",
        "eh_feriado",
        "eh_dia_util",
        "holiday_name",
    )
)

(
    df_dim_data.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_data")
)
print(f"[OK] {GOLD_SCHEMA}.dim_data: {spark.table(f'{GOLD_SCHEMA}.dim_data').count()} linhas")
spark.table(f"{GOLD_SCHEMA}.dim_data").orderBy("data_completa").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo das dimensões

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.gold;

# COMMAND ----------

print("\n[GOLD - Dimensões] Resumo:")
print(f"{'Tabela':<25} {'Linhas':>10}")
print("-" * 40)

dims = ["dim_cliente", "dim_produto", "dim_canal", "dim_regiao", "dim_vendedor", "dim_data"]
for d in dims:
    count = spark.table(f"{GOLD_SCHEMA}.{d}").count()
    print(f"{d:<25} {count:>10,}")
