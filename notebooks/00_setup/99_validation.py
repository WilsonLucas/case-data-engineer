# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: 99_validation
# MAGIC ## Objetivo:
# MAGIC Smoke tests + reconciliação end-to-end do pipeline. Garante que os números batem do bronze ao gold via 7 testes: (1) contagem Bronze == Silver para entidades sem dedup, (2) soma `net_amount` Bronze == Silver == Gold para pedidos válidos, (3) cobertura referencial — todo `customer_code`/`seller_id`/`product_code` em fact_pedido existe nas dims, (4) cobertura temporal — todas as datas em fatos existem em `dim_data`, (5) distribuição `_dq_status` em cada Silver, (6) sanity check da `vw_kpi_business`, (7) time travel demo via DESCRIBE HISTORY (REQ-008). Executado como última task do DAG.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informação |
# MAGIC |--------|-------------|
# MAGIC | `workspace.bronze.*` | Counts e somas raw (referência da verdade) |
# MAGIC | `workspace.silver.*` | Counts pós-dedup e distribuição DQ |
# MAGIC | `workspace.gold.dim_*` / `fact_*` / `vw_kpi_business` | Reconciliação final |
# MAGIC
# MAGIC ## Histórico de alterações
# MAGIC | Data | Desenvolvido por | Modificações |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-08 | Wilson Lucas | Criação do notebook |
# MAGIC | 2026-05-10 | Wilson Lucas | Adapter UC: schemas `workspace.bronze/silver/gold` |
# MAGIC | 2026-05-11 | Wilson Lucas | Guards em `.collect()[0]` (REQ-BUG-001); time travel demo (REQ-008); DQ gate pos-fix dos bugs REQ-DQ-001/005 |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def safe_scalar(sql: str, attr: str = "total"):
    """Executa SQL e retorna o atributo da primeira linha; None se vazio.

    Evita IndexError quando spark.sql(...).collect() retorna lista vazia.
    Mantem o fluxo do notebook tolerante a tabelas faltantes ou vazias.
    """
    rows = spark.sql(sql).collect()
    if not rows:
        return None
    return getattr(rows[0], attr)


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
    bronze_count = spark.table(f"workspace.bronze.{ent}").count()
    try:
        silver_count = spark.table(f"workspace.silver.{ent}").count()
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
# MAGIC ## Teste 2 - Soma de net_amount Bronze -> Silver -> Gold (REQ-NF-002)

# COMMAND ----------

print("\n[TESTE 2] Soma de net_amount (FATURADO + EM_SEPARACAO, sem CANCELADO):")

bronze_sum = safe_scalar(
    """
    SELECT ROUND(SUM(CAST(REGEXP_REPLACE(net_amount, ',', '.') AS DECIMAL(15,2))), 2) AS total
    FROM workspace.bronze.pedidos_cabecalho
    WHERE UPPER(status_order) IN ('FATURADO', 'EM_SEPARACAO', 'EM SEPARACAO')
    """
)

silver_sum = safe_scalar(
    """
    SELECT ROUND(SUM(net_amount), 2) AS total
    FROM workspace.silver.pedidos_cabecalho
    WHERE status_canonico IN ('FATURADO', 'EM_SEPARACAO')
    """
)

gold_sum = safe_scalar(
    """
    SELECT ROUND(SUM(net_amount), 2) AS total
    FROM workspace.gold.fact_pedido
    WHERE status_canonico IN ('FATURADO', 'EM_SEPARACAO')
    """
)

print(f"  Bronze: {bronze_sum if bronze_sum is None else f'{bronze_sum:>15,.2f}'}")
print(f"  Silver: {silver_sum if silver_sum is None else f'{silver_sum:>15,.2f}'}")
print(f"  Gold:   {gold_sum if gold_sum is None else f'{gold_sum:>15,.2f}'}")

if bronze_sum == silver_sum == gold_sum and bronze_sum is not None:
    print("  [OK] Todos batem")
elif silver_sum == gold_sum and silver_sum is not None:
    diff = abs((bronze_sum or 0) - (silver_sum or 0))
    print(f"  [WARN] Silver/Gold batem, mas diff vs Bronze: {diff:.2f}")
else:
    print("  [FALHA] DIVERGENCIA detectada - investigar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste 3 - Cobertura referencial

# COMMAND ----------

print("\n[TESTE 3] Integridade referencial em workspace.gold.fact_pedido:")

orfaos_cliente = safe_scalar(
    """
    SELECT COUNT(*) AS qty
    FROM workspace.gold.fact_pedido p
    LEFT JOIN workspace.gold.dim_cliente c ON p.customer_code = c.customer_code
    WHERE c.customer_code IS NULL AND p.customer_code IS NOT NULL
    """,
    attr="qty",
)
print(f"  Pedidos com customer_code orfao: {orfaos_cliente}")

orfaos_vendedor = safe_scalar(
    """
    SELECT COUNT(*) AS qty
    FROM workspace.gold.fact_pedido p
    LEFT JOIN workspace.gold.dim_vendedor v ON p.seller_id = v.seller_id
    WHERE v.seller_id IS NULL AND p.seller_id IS NOT NULL
    """,
    attr="qty",
)
print(f"  Pedidos com seller_id orfao: {orfaos_vendedor}")

orfaos_produto = safe_scalar(
    """
    SELECT COUNT(*) AS qty
    FROM workspace.gold.fact_item i
    LEFT JOIN workspace.gold.dim_produto p ON i.product_code = p.product_code
    WHERE p.product_code IS NULL AND i.product_code IS NOT NULL
    """,
    attr="qty",
)
print(f"  Itens com product_code orfao: {orfaos_produto}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste 4 - Cobertura temporal

# COMMAND ----------

print("\n[TESTE 4] Cobertura temporal em dim_data:")

orfaos_data_pedido = safe_scalar(
    """
    SELECT COUNT(*) AS qty
    FROM workspace.gold.fact_pedido p
    LEFT JOIN workspace.gold.dim_data d ON p.data_id = d.data_id
    WHERE d.data_id IS NULL AND p.data_id IS NOT NULL
    """,
    attr="qty",
)
print(f"  fact_pedido sem data_id em dim_data: {orfaos_data_pedido}")

orfaos_data_entrega = safe_scalar(
    """
    SELECT COUNT(*) AS qty
    FROM workspace.gold.fact_entrega e
    LEFT JOIN workspace.gold.dim_data d1 ON e.data_envio_id = d1.data_id
    LEFT JOIN workspace.gold.dim_data d2 ON e.data_entrega_id = d2.data_id
    WHERE (e.data_envio_id IS NOT NULL AND d1.data_id IS NULL)
       OR (e.data_entrega_id IS NOT NULL AND d2.data_id IS NULL)
    """,
    attr="qty",
)
print(f"  fact_entrega com datas orfas em dim_data: {orfaos_data_entrega}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste 5 - Resumo DQ + Gate post-fix REQ-DQ-001/005

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

dq_distribution = {}
for t in silver_tables:
    try:
        df = spark.table(f"workspace.silver.{t}")
        if "_dq_status" not in df.columns:
            print(f"{t:<30} (sem _dq_status)")
            continue
        clean = df.filter(F.col("_dq_status") == "clean").count()
        warning = df.filter(F.col("_dq_status") == "warning").count()
        rejected = df.filter(F.col("_dq_status") == "rejected").count()
        dq_distribution[t] = {"clean": clean, "warning": warning, "rejected": rejected}
        print(f"{t:<30} {clean:>10,} {warning:>10,} {rejected:>10,}")
    except Exception as e:
        print(f"{t:<30} ERRO: {e}")

# Gate REQ-DQ-005: silver.ocorrencias NAO PODE ter rejected = 270 (over-flag bug)
ocorr_rejected = dq_distribution.get("ocorrencias", {}).get("rejected", 0)
ocorr_total = sum(dq_distribution.get("ocorrencias", {}).values())
if ocorr_total == 270 and ocorr_rejected == 270:
    print("\n  [FALHA REQ-DQ-005] BUG over-flag persiste: 270/270 ocorrencias rejected")
elif ocorr_total > 0:
    pct_rejected = 100.0 * ocorr_rejected / ocorr_total
    print(f"\n  [OK REQ-DQ-005] silver.ocorrencias rejected: {ocorr_rejected}/{ocorr_total} ({pct_rejected:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste 6 - Sanity check da view consolidada

# COMMAND ----------

print("\n[TESTE 6] Sanity check de workspace.gold.vw_kpi_business:")

vw_count = safe_scalar("SELECT COUNT(*) AS total FROM workspace.gold.vw_kpi_business")
fact_count = safe_scalar("SELECT COUNT(*) AS total FROM workspace.gold.fact_pedido")

print(f"  vw_kpi_business: {vw_count if vw_count is None else f'{vw_count:,} linhas'}")
print(f"  fact_pedido:     {fact_count if fact_count is None else f'{fact_count:,} linhas'}")

if vw_count is not None and fact_count is not None and vw_count == fact_count:
    print("  [OK] View granular pedido OK")
elif vw_count is not None and fact_count is not None:
    print(f"  [WARN] Divergencia: view tem {vw_count - fact_count:+,} linhas a mais/menos que fact_pedido")
else:
    print("  [FALHA] Tabela ou view ausente - verificar pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teste 7 - Time Travel Delta (REQ-008)

# COMMAND ----------

print("\n[TESTE 7] Time travel demo em workspace.gold.fact_pedido:")

# DESCRIBE HISTORY mostra todas as versoes Delta com timestamp + operacao
print("\n  Historico de versoes (ultimas 5):")
history = spark.sql(
    "SELECT version, timestamp, operation, operationMetrics.numOutputRows as rows "
    "FROM (DESCRIBE HISTORY workspace.gold.fact_pedido) "
    "ORDER BY version DESC LIMIT 5"
).collect()

for h in history:
    rows_str = f"{int(h.rows):>6,}" if h.rows else "      "
    print(f"    v{h.version:<3} {h.timestamp} | {h.operation:<10} | {rows_str} rows")

# Query versao zero (primeira escrita)
v0_count = safe_scalar("SELECT COUNT(*) AS total FROM workspace.gold.fact_pedido VERSION AS OF 0")
current_count = safe_scalar("SELECT COUNT(*) AS total FROM workspace.gold.fact_pedido")
print(f"\n  fact_pedido VERSION AS OF 0: {v0_count} linhas")
print(f"  fact_pedido atual:           {current_count} linhas")

# COMMAND ----------

print("\n" + "=" * 80)
print("VALIDACAO CONCLUIDA")
print("=" * 80)
print("\nProximo passo: revisar warnings, executar queries de business_questions.md")
