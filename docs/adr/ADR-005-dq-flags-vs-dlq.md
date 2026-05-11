# ADR-005: DQ flags inline + Quarantine tables (alternativa a DLT)

## Status

Aceito — 2026-05-11. Esta ADR documenta o pattern principal de Data Quality do pipeline e referencia 2 bugs críticos descobertos durante auditoria forense.

## Contexto

A camada Silver precisa lidar com qualidade variável dos dados originados de 9 fontes heterogêneas. Auditoria forense identificou 51 issues distintos de DQ — desde formatos inconsistentes (decimal `1.234,56` vs `1234.56`) até integridade comprometida (FKs órfãs, datas inválidas, valores fora de domínio).

A escolha do pattern de DQ tem implicações profundas em:
- Auditabilidade: posso rastrear por que um registro foi rejeitado?
- Reprocessabilidade: rejeições podem ser revisadas e curadas?
- Performance: o pipeline para por causa de um registro ruim?
- Visibilidade ao consumidor: o BI sabe o que foi excluído?

Quatro padrões foram avaliados:

A. **Fail-fast**: pipeline para no primeiro registro inválido. Simplest mas operacionalmente inviável (1 row ruim mata o batch).

B. **Drop silencioso**: filtrar registros ruins sem logar. Pipeline roda mas perde rastreabilidade — auditor não sabe o que foi excluído.

C. **DQ flags inline (`_dq_status` + `_dq_reasons` no próprio Silver)**: marcar registros mas não excluir. Auditável; consumidor decide se filtra ou não. Risco: rejected "polui" o silver clean — consumidor desatento agrega rejected.

D. **DQ flags + quarantine table separada**: marcar + isolar rejected em tabela paralela `quarantine_<entity>`. Auditável + isolado. Pattern equivalente a DLT quarantine, implementado manualmente em PySpark.

O Free Edition não oferece DLT (Delta Live Tables — Premium-only, ADR-004). Implementar D manualmente é o equivalente arquitetural mais próximo.

## Decisão

Adotar **D — DQ flags inline + Quarantine tables**:

### Schema das colunas DQ no Silver

Cada tabela Silver tem 2 colunas DQ obrigatórias:

- `_dq_status` STRING NOT NULL, enum: `clean`, `warning`, `rejected`.
- `_dq_reasons` ARRAY<STRING> — array de razões legíveis (`["email inválido", "uf desconhecida"]`).

### Classificação por TIPO de issue (não por contagem)

A primeira versão do pipeline classificava por contagem (`size <= 2 → warning, else → rejected`), o que é arbitrário. A nova classificação é por TIPO:

- **PK / FK / null obrigatório** → `rejected` (registro inutilizável para joins).
- **Formato inválido / enum não-canônico** → `warning` (registro analisável mas com nota).
- **Sem issues** → `clean`.

### Quarantine pattern — split no `bronze_df` ANTES dos casts

A regra crítica: o split `rejected → quarantine`, `clean+warning → silver` acontece sobre o `bronze_df` (string-typed, ADR-001), **antes** de qualquer cast ou transformação. Isso garante que a quarantine table preserva o **valor original** do registro problemático — auditor pode debugar exatamente o que estava na fonte.

```python
# Padrão obrigatório em todos os silvers
# Schemas: workspace.bronze, workspace.silver (sem prefixo case_levva_, ADR-003)
bronze_df = spark.table(f"workspace.bronze.{ENTITY}")
dq_df = bronze_df.withColumn("_dq_reasons", ...).withColumn("_dq_status", ...)

quarantine_df = (
    dq_df.filter(F.col("_dq_status") == "rejected")
    .select(*BRONZE_COLS, "_source_file", "_ingestion_timestamp")
    .withColumn("_quarantine_timestamp", F.current_timestamp())
    .withColumn("_quarantine_reason", F.concat_ws(" | ", F.col("_dq_reasons")))
)
quarantine_df.write.format("delta").mode("overwrite").saveAsTable(
    f"workspace.silver.quarantine_{ENTITY}"
)

silver_df = (
    dq_df.filter(F.col("_dq_status") != "rejected")
    .withColumn("col_typed", F.expr(f"try_cast(col_raw as decimal(15,2))"))
    # ... casts ...
)
silver_df.write.format("delta").mode("overwrite").saveAsTable(f"workspace.silver.{ENTITY}")
```

### Schema da quarantine table

Colunas originais do Bronze (string-typed, preservadas) + 4 metadados:

| Coluna | Tipo | Propósito |
|--------|------|-----------|
| `_quarantine_timestamp` | TIMESTAMP | Quando foi quarentenado |
| `_quarantine_reason` | STRING | Razões concatenadas (ex: `"email inválido | uf ausente"`) |
| `_source_file` | STRING | Rastreabilidade até o arquivo de origem |
| `_ingestion_timestamp` | TIMESTAMP | Offset temporal da ingestão original |

### Retenção

`delta.deletedFileRetentionDuration = interval 7 days` (ADR-007). Quarantine VACUUM é operação manual (não-automática), executada quando análise de issues for concluída.

## Bugs críticos descobertos durante auditoria

Durante a auditoria forense via SDD (`/define-m`), 2 bugs críticos foram identificados em direções opostas:

### BUG #1 (under-flag) — `02_silver_pedidos.py:126`

```python
# ANTES (bug):
F.when(F.size("_dq_reasons") == 0, F.lit("clean"))
.when(F.size("_dq_reasons") <= 2, F.lit("warning"))
.otherwise(F.lit("warning"))   # NUNCA chega em "rejected"

# DEPOIS (corrigido):
F.when(F.size("_dq_reasons") == 0, F.lit("clean"))
.when(F.size("_dq_reasons") <= 2, F.lit("warning"))
.otherwise(F.lit("rejected"))
```

Resultado pré-fix: tabela sumária declarava "DQ rejected = 0" para pedidos. Falso — era consequência do bug, não dos dados limpos.

### BUG #2 (over-flag) — `02_silver_ocorrencias.py:92`

```python
# ANTES (bug):
F.when(
    ~F.col("event_type").isin("REFUND", "TROCA", "DELAY", "COMPLAINT", "NAO_CLASSIFICADO"),
    F.lit(F.concat(F.lit("event_type não canônico: "), F.col("event_type"))),
).otherwise(F.lit(None))

# DEPOIS (corrigido):
F.when(
    ~F.col("event_type").isin("REFUND", "TROCA", "DELAY", "COMPLAINT", "NAO_CLASSIFICADO"),
    F.concat(F.lit("event_type não canônico: "), F.col("event_type")),
).otherwise(F.lit(None))
```

Causa: `F.lit()` envolvendo uma `Column` (`F.concat(...)` retorna Column) força serialização da expressão como literal estático sempre não-null. Resultado pré-fix: 270/270 tickets caíam em rejected (atingiam 3+ razões falsamente). Fix: `F.concat()` já retorna Column válida — `F.lit()` é desnecessário e prejudicial.

### Lição aprendida

`F.lit()` é para valores Python literais (strings, numbers, booleans), **nunca** para envolver expressões Column existentes. Code review futuro deve grep por padrões `F.lit(F.<func>(...))` como red flag.

## Alternativas rejeitadas

1. **DLT (Delta Live Tables)**: rejeitada por ambiente — Free Edition não oferece (ADR-004). DQ flags + quarantine é o equivalente arquitetural.

2. **Fail-fast**: rejeitada — operacionalmente inviável.

3. **Drop silencioso sem flags**: rejeitada — perde auditabilidade; consumidor não sabe o que foi excluído.

4. **DQ flags inline sem quarantine separada**: rejeitada — rejected fica no silver clean, pode poluir agregações.

5. **Great Expectations / Soda Core como camada de DQ**: rejeitada por escopo — adicionaria 1 dependência externa e tooling sem ganho proporcional para o caso. Em produção com SLA estrito, Great Expectations seria recomendado.

## Consequências

**Positivas:**

- Pipeline nunca para por dado ruim — tolerante a falhas operacionais.
- Auditoria forense viável: cada rejected tem `_quarantine_reason` legível + valor original preservado.
- Equivalente arquitetural a DLT — narrativa defensável em entrevista ("em DLT seria expectations declarativas; aqui implementamos manualmente").
- Quarantine isolada protege Silver clean de agregações incorretas.
- Padrão consistente em todos os 8 silvers.

**Negativas:**

- Mais escrita por silver (2 writes ao invés de 1) — pequeno overhead de I/O.
- Dependência de disciplina manual: cada novo silver precisa replicar o pattern (mitigado pelo Padrão 1 documentado em `DESIGN.md`).
- Quarantine tables aumentam contagem total de tabelas (4-8 quarantine adicionais).
- Fix dos bugs descobertos pode mudar distribuição DQ (premissa A-009 do DEFINE) — re-validação obrigatória pós-fix.

**Trade-off aceito:** complexidade adicional de pattern manual > dependência em DLT (não disponível) ou perda de auditabilidade.

## Referências

- ADR-001 (Bronze string-typed — fonte de dados para quarantine)
- ADR-004 (Free Edition vs Premium — justifica ausência de DLT)
- ADR-007 (retenção Delta — política de VACUUM da quarantine)
- `notebooks/02_silver/02_silver_*.py` (8 implementações do pattern)
- `docs/data_quality.md` — 51 issues mapeadas
- DEFINE REQ-DQ-001 (bug under-flag) e REQ-DQ-005 (bug over-flag)
