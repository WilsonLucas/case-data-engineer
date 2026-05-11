# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: 00_rename_schemas (migracao one-shot)
# MAGIC ## Objetivo:
# MAGIC Renomear schemas removendo prefixo redundante `case_levva_`. Pre-flight de capabilities Free Edition + drop dos schemas antigos + create dos novos + recriar Volume `landing.sources`. Executar uma unica vez. Decisao 10 do DESIGN; ADR-003 atualizado com nova nomenclatura.
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informacao |
# MAGIC |--------|-------------|
# MAGIC | `workspace.case_levva.sources` | Volume atual (sera dropado e recriado em landing) |
# MAGIC | `system.information_schema.schemata` | Verificacao do estado atual |
# MAGIC
# MAGIC ## Historico de alteracoes
# MAGIC | Data | Desenvolvido por | Modificacoes |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-11 | Wilson Lucas | Criacao do notebook de migracao schemas (Decisao 10 DESIGN uplift 200%) |

# COMMAND ----------

# 1. Pre-flight checks
print("[PRE-FLIGHT] Verificando capabilities do Free Edition...")

# Versao do runtime serverless
runtime_version = spark.sql("SELECT current_version() as v").first().v
print(f"[OK] Runtime version: {runtime_version}")

# Schemas atuais (antes da migracao)
current_schemas = spark.sql(
    "SELECT schema_name FROM system.information_schema.schemata "
    "WHERE catalog_name = 'workspace' AND schema_name LIKE 'case_levva%'"
).collect()
print(f"[OK] Schemas com prefixo case_levva atualmente existentes: {[r.schema_name for r in current_schemas]}")

# Tabelas existentes em cada schema
for schema in ["case_levva", "case_levva_bronze", "case_levva_silver", "case_levva_gold"]:
    try:
        tables = spark.sql(
            f"SELECT table_name FROM system.information_schema.tables "
            f"WHERE table_catalog = 'workspace' AND table_schema = '{schema}'"
        ).collect()
        print(f"[OK] {schema}: {len(tables)} tabelas - {[r.table_name for r in tables][:5]}{'...' if len(tables) > 5 else ''}")
    except Exception as e:
        print(f"[WARN] {schema}: {e}")

# COMMAND ----------

# 2. Drop schemas antigos (CASCADE remove tabelas + Volumes)
print("\n[DROP] Removendo schemas antigos...")
for schema in ["case_levva_bronze", "case_levva_silver", "case_levva_gold", "case_levva"]:
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS workspace.{schema} CASCADE")
        print(f"[OK] DROP SCHEMA workspace.{schema} CASCADE")
    except Exception as e:
        print(f"[ERRO] DROP SCHEMA workspace.{schema}: {e}")
        raise

# COMMAND ----------

# 3. Create schemas novos (nomenclatura canonica Medallion + landing)
print("\n[CREATE] Criando schemas com nomenclatura canonica...")

new_schemas = {
    "landing": "Camada landing - hospeda Volume sources com arquivos brutos das 9 fontes",
    "bronze": "Camada Bronze - dados brutos string-typed (ADR-001)",
    "silver": "Camada Silver - dados tipados, deduplicados, com DQ flags + quarantine (ADR-005)",
    "gold": "Camada Gold - star schema dimensional para consumo BI (ADR-002)",
}

for schema, comment in new_schemas.items():
    spark.sql(f"CREATE SCHEMA workspace.{schema} COMMENT '{comment}'")
    print(f"[OK] CREATE SCHEMA workspace.{schema}")

# COMMAND ----------

# 4. Recriar Volume sources em landing
print("\n[VOLUME] Criando Volume managed workspace.landing.sources...")
spark.sql(
    "CREATE VOLUME workspace.landing.sources "
    "COMMENT 'Volume managed para arquivos brutos das 9 fontes do case Levva'"
)
print("[OK] Volume workspace.landing.sources criado")

# COMMAND ----------

# 5. Verificacao final
print("\n[VERIFICACAO] Estado final dos schemas:")
final_schemas = spark.sql(
    "SELECT schema_name, comment FROM system.information_schema.schemata "
    "WHERE catalog_name = 'workspace' "
    "ORDER BY schema_name"
).collect()
for row in final_schemas:
    print(f"  - workspace.{row.schema_name}: {row.comment or '(sem comentario)'}")

print("\n[OK] Migracao concluida. Proximos passos:")
print("  1. Re-upload das 9 fontes para /Volumes/workspace/landing/sources/ via databricks fs cp")
print("  2. Search/replace global em notebooks (case_levva_bronze -> bronze, etc)")
print("  3. Re-deploy notebooks via databricks workspace import")
print("  4. Re-rodar pipeline e validar reconciliacao R$ 1.707.675,84")
