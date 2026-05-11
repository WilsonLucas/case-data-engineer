# ADR-004: Databricks Free Edition vs Premium — limitações conscientes

## Status

Aceito — 2026-05-11. Documenta restrições do ambiente que afetam diretamente o design do pipeline.

## Contexto

O case foi desenvolvido no **Databricks Free Edition** (substitui o Community Edition descontinuado em junho/2025). O Free Edition oferece Unity Catalog, Delta Lake, serverless compute, multi-task jobs e SQL warehouses serverless — suficiente para implementar a arquitetura Medallion completa. Porém, recursos Premium-only afetam decisões arquiteturais e precisam ser explicitamente documentados para que um avaliador senior compreenda o que foi possível, o que foi simulado e o que ficou explicitamente fora de escopo.

A enunciação do PDF do case menciona "Databricks Community Edition", mas o produto foi descontinuado meses antes. Documentar a substituição por Free Edition é parte da entrega.

## Decisão

Adotar **Databricks Free Edition** com as seguintes restrições documentadas e mitigações arquiteturais explícitas:

### Recursos disponíveis no Free Edition (usados ativamente)

- Unity Catalog habilitado por padrão (catálogo `workspace`).
- Serverless compute (sem cluster próprio).
- UC Volumes managed (substituem DBFS legado).
- Multi-task jobs com DAG (`databricks jobs submit --json @pipeline_dag.json`).
- SQL warehouses serverless.
- Tokens com retenção de 90 dias.
- Tags em tabelas (`ALTER TABLE SET TAGS`) e queryable via `system.information_schema.table_tags`.
- COMMENT ON TABLE/COLUMN.
- CHECK constraints e NOT NULL constraints em Delta.
- FK informational (`RELY NOVALIDATE`).
- Time travel via `DESCRIBE HISTORY` e `VERSION AS OF`.
- Lineage automática no Catalog Explorer.

### Recursos Premium-only (NÃO usados — fora de escopo)

- **DLT (Delta Live Tables)**: pipeline declarativo com gerenciamento automático de DQ. Substituído por DQ flags + quarantine pattern (ADR-005) implementados manualmente em PySpark.
- **Workflows agendados (cron)**: jobs só executam on-demand via `databricks jobs run-now` ou via UI. Em produção, o pipeline rodaria daily/hourly via Workflow scheduled trigger.
- **RBAC granular**: `GRANT`/`REVOKE` executam sem erro mas são no-op no Free Edition single-user. O owner do workspace tem todos os privilégios em todos os objetos. Em produção, GRANTs por team/role seriam configurados.
- **Asset Bundles (`databricks bundle`)**: gestão de configurações multi-environment (dev/stage/prod). Substituído por GitHub Actions de lint + py_compile.
- **`system.access.audit_log`**: tabela de audit log indisponível no Free Edition. Em produção, queries de auditoria de quem acessou o quê seriam viáveis.
- **Cluster próprio (não-serverless)**: não disponível. Toda computação é serverless. Impacto: sem capacidade de instalar libs custom no init script; usar `%pip install` no notebook.
- **Concurrency ilimitada de tasks paralelas**: Free Edition serializa silvers paralelos quando estoura cota de concorrência. DAG continua correto, apenas serializa. Wall-clock medido: ~25 minutos para o pipeline completo.

### Mitigações arquiteturais

| Limitação | Mitigação |
|-----------|-----------|
| Sem DLT | DQ flags `_dq_status` + quarantine tables manuais (ADR-005) — equivalente arquitetural |
| Sem Workflows agendados | DAG roda on-demand via CLI; documentar em README como "em produção, agendar via Workflow trigger" |
| Sem RBAC granular | Tags `owner` + `classification` + `pii` aplicadas via `ALTER TABLE SET TAGS` (ADR-008); `GRANT` documentado como código comentado com nota "requires Premium" |
| Sem Asset Bundles | GitHub Actions com lint + py_compile (CI mínimo); ADR-006 (CI strategy) documenta |
| Sem audit_log | Query exemplo documentada em `99_validation.py` com nota "requires Premium audit log entitlement" |
| Cota de concorrência limitada | Aceitar serialização; medir wall-clock e otimizar apenas se estourar SLA |

### Custo

Free Edition é gratuito (sem cobrança de DBU). Para defesa do case: estimativa de custo em produção Serverless Jobs (`~$0.07/DBU` referência mid-2025) — pipeline completo (~23 min, ~1.5 DBU) custaria aproximadamente **$0.10 USD por run** na região sa-east-1. Em volume de 1 execução diária, custo mensal seria inferior a $3 USD.

## Alternativas rejeitadas

1. **Migrar para trial Premium (14 dias)**: rejeitado — exigiria reescrita de pipeline para usar DLT, Asset Bundles e Workflows; 14 dias é insuficiente para entregar com qualidade; custo recorrente após trial é alto.

2. **Usar outro provedor (Snowflake, BigQuery free tier)**: rejeitado — case especifica Databricks; mudar provedor invalida o case.

3. **Self-hosted Spark com Delta Lake OSS**: rejeitado — sem Unity Catalog (perda de governança visível), sem Catalog Explorer UI, perda de demonstração de UC.

## Consequências

**Positivas:**

- Toda a arquitetura é reproduzível por qualquer pessoa com conta Free Edition gratuita — zero barreira de entrada para o avaliador.
- Decisões de fora de escopo são explícitas e justificadas, demonstrando consciência das limitações em vez de tentar mascarar.
- Em entrevista oral, candidato pode argumentar com fluência: "em Premium faríamos X; em Free Edition fizemos Y porque...".
- Tags + COMMENT em UC funcionam em Free Edition — governança real, não apenas documentação externa.

**Negativas:**

- Avaliador pode perguntar "por que não Premium?" — resposta documentada: trial não atenderia o prazo + pipeline foi pensado para reproduzibilidade; Premium features são roadmap claro para produção.
- Padrões de produção (Workflows, RBAC, Asset Bundles) ficam apenas como argumento oral, sem implementação visível.
- Wall-clock pode ser maior do que em Premium (cota de concorrência).

**Trade-off aceito:** demonstrar fluência arquitetural sem feature lock-in > usar features Premium não acessíveis ao avaliador.

## Roadmap em Premium (para defesa oral)

Se este pipeline fosse promovido a produção em Premium, as primeiras 5 ações seriam:

1. Converter pipeline para DLT — DQ expectations declarativas substituem `_dq_status` flags manuais.
2. Configurar Asset Bundles — versionamento de pipeline por environment (dev/stage/prod).
3. Workflow scheduled trigger — pipeline roda daily às 03:00 BRT após ETL upstream.
4. RBAC: GRANT SELECT em `case_levva_gold.*` para grupo `bi-analysts`; REVOKE em Bronze e Silver.
5. Configurar `system.access.audit_log` queries em dashboard de governança.

## Referências

- ADR-005 (DQ flags + quarantine — equivalente arquitetural a DLT no Free Edition)
- ADR-006 (CI mínimo via GitHub Actions — equivalente a Asset Bundles)
- `data/reference_databricks_free_edition.md` (memory) — gotchas comprovados em produção
- Databricks "Free Edition vs Community Edition" announcement (junho/2025)
