# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: 99_validation
# MAGIC ## Objetivo:
# MAGIC Smoke tests + reconciliação end-to-end do pipeline. Garante que os números batem do bronze ao gold via 6 testes: (1) contagem Bronze == Silver para entidades sem dedup, (2) soma `net_amount` Bronze == Silver == Gold para pedidos válidos, (3) cobertura referencial — todo `customer_code`/`seller_id`/`product_code` em fact_pedido existe nas dims, (4) cobertura temporal — todas as datas em fatos existem em `dim_data`, (5) distribuição `_dq_status` em cada Silver, (6) sanity check da `vw_kpi_business`. Executado como última task do DAG.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `workspace.case_levva_bronze.*` | Counts e somas raw (referência da verdade) |
# MAGIC | `workspace.case_levva_silver.*` | Counts pós-dedup e distribuição DQ |
# MAGIC | `workspace.case_levva_gold.dim_*` / `fact_*` / `vw_kpi_business` | Reconciliação final |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC: schemas `workspace.case_levva_bronze/silver/gold` |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

print("=" * 80)
print("VALIDACAO END-TO-END")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste 1 - Contagem de linhas Bronze vs Silver
# MAGIC
# MAGIC Para entidades sem dedup, contagens devem bater. Para entidades com dedup (vendedores, regioes), Silver < Bronze.

# COMMAND ----------

print("\n[TESTE 1] Contagem Bronze vs Silver:")
print(f"{'Entidade':<25} {'Bronze':>10} {'Silver':>10} {'Esperado':<25}")
print("-" * 75)

entidades = [
    ("pedidos_cabecalho", "exato"),
    ("pedidos_itens", "exato"),
    ("produtos", "exato"),
    ("clientes", "exato"),
    ("canais", "exato"),
    ("vendedores", "Silver < Bronze (dedup)"),
    ("regioes", "Silver < Bronze (dedup)"),
    ("ocorrencias", "exato"),
    ("entregas", "exato"),
]

for ent, esperado in entidades:
    bronze_count = spark.table(f"workspace.case_levva_bronze.{ent}").count()
    try:
        silver_count = spark.table(f"workspace.case_levva_silver.{ent}").count()
    except Exception:
        print(f"{ent:<25} {bronze_count:>10,} {'ERRO':>10} {esperado:<25}")
        continue

    ok = (esperado == "exato" and bronze_count == silver_count) or (
        esperado.startswith("Silver <") and silver_count < bronze_count
    )
    status = "OK" if ok else "FALHA"
    print(f"{ent:<25} {bronze_count:>10,} {silver_count:>10,} {esperado:<25} {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste 2 - Soma de net_amount Bronze -> Silver -> Gold

# COMMAND ----------

print("\n[TESTE 2] Soma de net_amount (FATURADO + EM_SEPARACAO, sem CANCELADO):")

bronze_sum = spark.sql("""
    SELECT ROUND(SUM(CAST(REGEXP_REPLACE(net_amount, ',', '.') AS DECIMAL(15,2))), 2) AS total
    FROM workspace.case_levva_bronze.pedidos_cabecalho
    WHERE UPPER(status_order) IN ('FATURADO', 'EM_SEPARACAO', 'EM SEPARACAO')
    """).collect()[0].total

silver_sum = spark.sql("""
    SELECT ROUND(SUM(net_amount), 2) AS total
    FROM workspace.case_levva_silver.pedidos_cabecalho
    WHERE status_canonico IN ('FATURADO', 'EM_SEPARACAO')
    """).collect()[0].total

gold_sum = spark.sql("""
    SELECT ROUND(SUM(net_amount), 2) AS total
    FROM workspace.case_levva_gold.fact_pedido
    WHERE status_canonico IN ('FATURADO', 'EM_SEPARACAO')
    """).collect()[0].total

print(f"  Bronze: {bronze_sum:>15,.2f}")
print(f"  Silver: {silver_sum:>15,.2f}")
print(f"  Gold:   {gold_sum:>15,.2f}")

if bronze_sum == silver_sum == gold_sum:
    print("  [OK] Todos batem")
elif silver_sum == gold_sum:
    print(f"  [WARN] Silver/Gold batem, mas diff vs Bronze: {abs(bronze_sum - silver_sum):.2f}")
else:
    print(f"  [FALHA] DIVERGENCIA detectada - investigar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste 3 - Cobertura referencial

# COMMAND ----------

print("\n[TESTE 3] Integridade referencial em workspace.case_levva_gold.fact_pedido:")

orfaos_cliente = spark.sql("""
    SELECT COUNT(*) AS qty
    FROM workspace.case_levva_gold.fact_pedido p
    LEFT JOIN workspace.case_levva_gold.dim_cliente c ON p.customer_code = c.customer_code
    WHERE c.customer_code IS NULL AND p.customer_code IS NOT NULL
    """).collect()[0].qty
print(f"  Pedidos com customer_code orfao: {orfaos_cliente}")

orfaos_vendedor = spark.sql("""
    SELECT COUNT(*) AS qty
    FROM workspace.case_levva_gold.fact_pedido p
    LEFT JOIN workspace.case_levva_gold.dim_vendedor v ON p.seller_id = v.seller_id
    WHERE v.seller_id IS NULL AND p.seller_id IS NOT NULL
    """).collect()[0].qty
print(f"  Pedidos com seller_id orfao: {orfaos_vendedor}")

orfaos_produto = spark.sql("""
    SELECT COUNT(*) AS qty
    FROM workspace.case_levva_gold.fact_item i
    LEFT JOIN workspace.case_levva_gold.dim_produto p ON i.product_code = p.product_code
    WHERE p.product_code IS NULL AND i.product_code IS NOT NULL
    """).collect()[0].qty
print(f"  Itens com product_code orfao: {orfaos_produto}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste 4 - Cobertura temporal

# COMMAND ----------

print("\n[TESTE 4] Cobertura temporal em dim_data:")

orfaos_data_pedido = spark.sql("""
    SELECT COUNT(*) AS qty
    FROM workspace.case_levva_gold.fact_pedido p
    LEFT JOIN workspace.case_levva_gold.dim_data d ON p.data_id = d.data_id
    WHERE d.data_id IS NULL AND p.data_id IS NOT NULL
    """).collect()[0].qty
print(f"  fact_pedido sem data_id em dim_data: {orfaos_data_pedido}")

orfaos_data_entrega = spark.sql("""
    SELECT COUNT(*) AS qty
    FROM workspace.case_levva_gold.fact_entrega e
    LEFT JOIN workspace.case_levva_gold.dim_data d1 ON e.data_envio_id = d1.data_id
    LEFT JOIN workspace.case_levva_gold.dim_data d2 ON e.data_entrega_id = d2.data_id
    WHERE (e.data_envio_id IS NOT NULL AND d1.data_id IS NULL)
       OR (e.data_entrega_id IS NOT NULL AND d2.data_id IS NULL)
    """).collect()[0].qty
print(f"  fact_entrega com datas orfas em dim_data: {orfaos_data_entrega}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste 5 - Resumo DQ

# COMMAND ----------

print("\n[TESTE 5] Distribuicao de _dq_status em cada Silver:")
print(f"{'Tabela':<30} {'clean':>10} {'warning':>10} {'rejected':>10}")
print("-" * 65)

silver_tables = [
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

for t in silver_tables:
    try:
        df = spark.table(f"workspace.case_levva_silver.{t}")
        if "_dq_status" not in df.columns:
            print(f"{t:<30} (sem _dq_status)")
            continue
        clean = df.filter(F.col("_dq_status") == "clean").count()
        warning = df.filter(F.col("_dq_status") == "warning").count()
        rejected = df.filter(F.col("_dq_status") == "rejected").count()
        print(f"{t:<30} {clean:>10,} {warning:>10,} {rejected:>10,}")
    except Exception as e:
        print(f"{t:<30} ERRO: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste 6 - Sanity check da view consolidada

# COMMAND ----------

print("\n[TESTE 6] Sanity check de workspace.case_levva_gold.vw_kpi_business:")

vw_count = spark.sql("SELECT COUNT(*) AS total FROM workspace.case_levva_gold.vw_kpi_business").collect()[0].total
fact_count = spark.sql("SELECT COUNT(*) AS total FROM workspace.case_levva_gold.fact_pedido").collect()[0].total

print(f"  vw_kpi_business: {vw_count:,} linhas")
print(f"  fact_pedido:     {fact_count:,} linhas")

if vw_count == fact_count:
    print("  [OK] View granular pedido OK")
else:
    print(f"  [WARN] Divergencia: view tem {vw_count - fact_count:+,} linhas a mais/menos que fact_pedido")

# COMMAND ----------

print("\n" + "=" * 80)
print("VALIDACAO CONCLUIDA")
print("=" * 80)
print("\nProximo passo: revisar warnings, executar queries de business_questions.md")
