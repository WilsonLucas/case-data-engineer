# SHIPPED: Case Engenheiro de Dados Senior - Uplift 200%

**Data:** 2026-05-12
**Repositorio:** https://github.com/WilsonLucas/case-data-engineer
**Workflow:** SDD completo (`/brainstorm` -> `/define-m` -> `/design-m` -> `/build` -> `/ship`)

## Resumo de entrega

Pipeline Medallion 100% verde sobre Databricks Free Edition + Unity Catalog + Delta Lake, com governanca UC viva, modelagem dimensional avancada (incluindo SCD Type 2 demonstrativa), suite de testes pytest e 5 ADRs Nygard documentando decisoes arquiteturais.

### Volumes

- **9 fontes brutas** ingeridas via UC Volume `workspace.landing.sources`
- **28 tabelas Delta** (9 bronze + 9 silver + 11 gold) + 1 view consolidada
- **403 pedidos**, 995 itens, 72 produtos, 180 clientes, 7 canais, 6 regioes, 40 vendedores, 325 entregas, 270 ocorrencias
- **R\$ 1.707.675,84** reconciliacao Bronze=Silver=Gold mantida apos rename schemas + 3 fixes de bug

### Metricas finais

| Metrica | Valor |
|---------|-------|
| Wall-clock pipeline end-to-end | ~6 minutos (Free Edition serverless) |
| Tabelas com COMMENT non-empty | 100% (28/28) |
| Colunas business gold com COMMENT | 100% |
| Tags UC fixas por tabela | 5 (owner, layer, classification, pii, data_domain) |
| ADRs Nygard PT-BR | 5 |
| Documentos de apoio | 8 (TABLES, GLOSSARY, NAMING_CONVENTIONS, BI_RUNBOOK, data_model, data_quality, business_questions, architecture) |
| Testes pytest+chispa | 15 casos (Camada 1 Python puro + Camada 2 Spark local) |
| HTML deliverables | 2 (slides magazine-quality 12 paginas + diagrama interativo) |

## Lessons Learned

### O que funcionou bem

1. **Workflow SDD completo via slashes** (`/brainstorm` -> `/define-m` -> `/design-m` -> `/build` -> `/ship`) trouxe rastreabilidade total das decisoes. Cada fase produziu artefato consultavel (BRAINSTORM, DEFINE, DESIGN com 9 decisoes + risk register, BUILD report implicito nos commits).

2. **Multi-agent na fase /define** descobriu o **bug under-flag** em `silver_pedidos.py:126` (data-quality-analyst) que estava mascarado em produção. Combinado com o bug over-flag de `silver_ocorrencias.py:92` (descoberto pelo Wilson via execucao real) revelou a **causa raiz**: `array_remove(arr, NULL)` retorna NULL em SQL (porque `NULL = NULL` eh `NULL`, nao `TRUE`). Solucao: substituir por `array_compact(arr)`.

3. **Decisao 10 (renomeacao de schemas)** tomada durante `/design-m` via screenshot do Catalog Explorer — `case_levva_*` -> `bronze`/`silver`/`gold`/`landing`. Aproveitou o re-run obrigatorio pos-bug-fix do Tier 5 (custo zero). Catalog Explorer ficou visualmente limpo, queries SQL mais curtas, pattern Medallion canonico.

4. **Script de governanca idempotente** com dict literal cobrindo 28 tabelas + ~80 colunas + fallback automatico para colunas nao mapeadas (via `fallback_comment`). Re-aplicar e seguro a qualquer momento.

5. **SCD2 demonstrativa em `dim_cliente_history`** com hash MD5 sobre 4 colunas tracking (segmento, UF, cidade, status) e MERGE pattern Delta. Range join sem surrogate key documentado em ADR-002 como trade-off consciente.

6. **`dim_data` enriquecida** com 12 feriados nacionais BR 2025 + `eh_dia_util` derivado + `semana_iso`. Permite `WHERE eh_dia_util=true` em queries de analise sem calculo runtime.

### O que faria diferente em outra iteracao

1. **Test strategy chispa offline em Windows**: setup do PySpark local com WinUtils foi adiado em favor de validacao via execucao real no Databricks. A suite pytest existe (`tests/test_data_helpers.py` com 15 casos) mas nao foi executada localmente nesta entrega. Para producao, configurar WSL2 ou usar GitHub Actions Linux para CI.

2. **Refactor dos 8 silvers para usar `notebooks/utils/data_helpers.py`**: as funcoes (`parse_multi_format_date`, `br_to_us_decimal`, `parse_multi_format_timestamp`) estao duplicadas em 5 silvers. Extraidas em `data_helpers.py` (com type hints + docstrings Google) mas a substituicao nos silvers ficou como follow-up (nao bloqueia funcionalidade).

3. **Quarantine tables explicitas em todos os silvers**: padrao documentado em ADR-005 mas implementacao real (split bronze_df antes de cast em escrita separada `silver.quarantine_<entity>`) nao foi aplicada. Os registros `_dq_status='rejected'` permanecem na tabela silver. Em producao, separar em quarantine isolada eh boa pratica.

4. **CI/CD via GitHub Actions**: `.github/workflows/lint.yml` e `databricks-validate.yml` foram especificados no DESIGN mas nao implementados. O `pyproject.toml` ja esta pronto com black + ruff + pytest config — adicionar workflow e setup secrets DATABRICKS_TOKEN.

5. **Logging estruturado + `pipeline_metrics` table**: especificado no DESIGN mas mantido como "follow-up". Notebooks ainda usam `print()` para output visual. Em producao, observability centralizada via tabela append-only seria essencial.

### Decisoes deferidas (YAGNI documentado)

- **Asset Bundles** (Premium feature, escopo reduzido)
- **DLT (Delta Live Tables)** (Free Edition nao suporta)
- **Workflows agendados** (Free Edition on-demand only)
- **Particionamento de fatos** (volume <1k rows nao justifica)
- **CDC nas fontes transacionais** (case eh one-shot)
- **dbt + Atlan integration** (escopo de outro projeto)
- **Surrogate keys nas dims** (range join SCD2 e defensavel para escopo do case)
- **Vidio demo** (slides HTML cobrem storytelling)

## Definition of Done atingida

### MUST (todos atendidos)

- [x] Repo publico em github.com/WilsonLucas/case-data-engineer acessivel
- [x] README.md com URL propria + links pra slides/diagrama HTML/ADRs
- [x] EXECUTIVE_SUMMARY.md com URL final
- [x] 5 ADRs em docs/adr/ com 4 secoes Nygard
- [x] 28 tabelas com COMMENT non-empty (gate REQ-001 = 0)
- [x] 100% das colunas business gold com COMMENT
- [x] 5 tags em todas as 28 tabelas (gate REQ-003 = 5+)
- [x] docs/{TABLES, GLOSSARY, NAMING_CONVENTIONS, BI_RUNBOOK}.md completos
- [x] Bug REQ-DQ-001 (under-flag silver_pedidos:126) corrigido
- [x] Bug REQ-DQ-005 (over-flag silver_ocorrencias:92) corrigido
- [x] Bug raiz `array_remove(arr, NULL) -> array_compact(arr)` em 6 silvers
- [x] 6 bug fixes de code audit aplicados
- [x] Zero emojis em codigo .py ou diagrama .mmd
- [x] Pipeline end-to-end verde com reconciliacao R\$ 1.707.675,84 mantida

### SHOULD (atendidos)

- [x] dim_cliente_history SCD2 demonstrada (169 rows is_current=true)
- [x] dim_data enriquecida (12 feriados BR 2025, eh_dia_util, semana_iso)
- [x] notebooks/utils/{data_helpers, config}.py com type hints + docstrings
- [x] Slides HTML magazine-quality (12 paginas)
- [x] Diagrama HTML interativo (28 tabelas com info de granularidade)
- [x] Time travel demo no 99_validation.py (DESCRIBE HISTORY + VERSION AS OF 0)
- [x] Bus matrix Kimball + SCD type por dim em data_model.md

### COULD (deferidos)

- [ ] CI workflows (.github/workflows/{lint, databricks-validate}.yml)
- [ ] Quarantine tables explicitas nos 8 silvers
- [ ] Refactor silvers para usar utils.data_helpers
- [ ] Logging estruturado + pipeline_metrics table populada
- [ ] Screenshots do Catalog Explorer (manuais — Wilson capturara antes de apresentacao)
- [ ] Suite pytest executada localmente (Windows requer setup adicional)

## Defesa oral - talking points

Pontos principais para entrevista tecnica:

1. **Por que Bronze como string-typed?** -> ADR-001 + ANSI mode estrito do Photon Spark 4.1.

2. **Por que SCD1 padrao + SCD2 so em cliente?** -> ADR-002 + custo/beneficio Free Edition + tracking de segmento/UF muda com tempo.

3. **Por que nao DLT?** -> ADR-005 (Free Edition nao tem) + DQ flags + quarantine = equivalente arquitetural. Em DLT seriam expectations declarativas; aqui implementamos manualmente.

4. **Como descobriu o bug raiz `array_remove(arr, NULL)`?** -> Auditoria forense via SDD identificou 2 bugs aparentemente nao relacionados (under-flag e over-flag). Investigacao SQL direta no warehouse mostrou que `NULL = NULL` eh `NULL` em SQL ANSI, nao `TRUE` — entao `array_remove(arr, NULL)` retorna NULL inteiro. Substituir por `array_compact()` resolveu ambos.

5. **Como o BI consome?** -> docs/BI_RUNBOOK.md (mostrar) + vw_kpi_business pre-joinada. Range join via dim_cliente_history para historico SCD2.

6. **Lineage UC?** -> Capturar no Catalog Explorer antes da apresentacao (manual). Lineage automatica entre Bronze->Silver->Gold via Spark plan.

7. **Observabilidade em producao?** -> pipeline_metrics table desenhada (notebooks/utils/config.py menciona); deploy ficou follow-up. CHECK constraints + COMMENT/TAGS no UC + DESCRIBE HISTORY ja oferecem governance basica.

8. **Como rodar?** -> Reproduzivel em qualquer Free Edition: gh clone + databricks workspace import-dir + jobs submit. Setup completo documentado no README.

## Follow-ups deferidos

Lista numerada com link para acoes futuras (ja em README "Proximos passos"):

1. Migracao Premium (DLT, Workflows, RBAC granular, audit_log)
2. CI/CD via Asset Bundles + GitHub Actions
3. Surrogate keys nas dims (eliminaria range join SCD2)
4. Particionamento por ano_mes (relevante a 10M+ rows)
5. CDC nas fontes transacionais
6. Quarantine tables explicitas nos 8 silvers
7. Refactor 8 silvers para usar utils.data_helpers
8. Logging estruturado + pipeline_metrics table populada
9. Service principal como owner (vs email pessoal)
10. dbt + Atlan para data catalog auto-sincronizado

## Artefatos do SDD (locais)

Arquivos de processo do workflow SDD permanecem fora do repo publico (em `.claude/sdd/features/case-levva-uplift-200/`):

- `BRAINSTORM_CASE_LEVVA_FORENSIC.md` — auditoria forense em 3 dimensoes + decisoes
- `DEFINE_CASE_LEVVA_FORENSIC.md` — ~50 REQs (MUST/SHOULD/COULD) com criterios de aceite mensuraveis
- `DESIGN_CASE_LEVVA_FORENSIC.md` — 10 decisoes arquiteturais + manifesto + risk register validado por specialists

ADRs (que SAO publicos) em `docs/adr/` no repo.
