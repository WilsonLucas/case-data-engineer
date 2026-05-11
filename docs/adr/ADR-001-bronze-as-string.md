# ADR-001: Bronze layer string-typed (sem casts no ingest)

## Status

Aceito — 2026-05-11. Aplicado em produção desde a primeira versão do pipeline.

## Contexto

O pipeline Medallion ingere 9 fontes brutas em formatos heterogêneos (CSV com separadores `;` e `,`, JSON aninhado, NDJSON, XLSX, TXT pipe). As fontes têm padrões de formato variáveis na mesma coluna conceitual: `order_date` chega às vezes como `2025-01-15`, às vezes `15/01/2025`, às vezes com hora; `net_amount` chega às vezes como `1.234,56` (BR), às vezes `1234.56` (US), às vezes `N/A`.

Duas estratégias se apresentaram para a camada Bronze:

A. **Bronze typed**: aplicar `cast()` ou `inferSchema=True` no momento da ingestão, materializando tipos finais (DECIMAL, DATE, TIMESTAMP) já no Bronze.

B. **Bronze string-typed**: ler todas as colunas como STRING, preservando o conteúdo original byte-a-byte. Os casts e parses ficam restritos à camada Silver.

A diferença não é meramente estilística. Em ambiente Databricks Free Edition (Photon Spark 4.1) com ANSI mode estrito ativado por padrão, qualquer cast falho lança `CAST_INVALID_INPUT` em runtime — interrompendo o job. `inferSchema=True` em CSV é não-determinístico em datasets pequenos e sensível a primeiras N linhas. Casts no Bronze também acoplam a ingestão à evolução de schema da fonte: uma coluna nova ou um valor inesperado quebra o ingest, não o silver.

## Decisão

Adotar **Bronze string-typed**. Todos os 9 notebooks de ingest leem com schema explícito de `StringType` ou inferência tolerante, sem casts. Tipos finais são responsabilidade exclusiva da camada Silver, onde:

- Decimais BR são parseados via `regexp_replace(col, ',', '.')` + `try_cast` via `F.expr` (ANSI safe — retorna NULL ao invés de lançar exceção).
- Datas multi-formato são parseadas via `F.coalesce(F.expr("try_to_date(col, 'fmt1')"), F.expr("try_to_date(col, 'fmt2')"), ...)`.
- Timestamps ISO 8601 com `'T'` literal são tratados via `replace(col, 'T', ' ')` antes de `try_to_timestamp` (workaround para limitações do DateTimeFormatter em Spark SQL escape).

Linhas que falham nos casts no Silver vão para `_dq_status='warning'` ou `_dq_status='rejected'` conforme severidade do campo (ver ADR-005), mas o registro original permanece preservado no Bronze.

## Consequências

**Positivas:**

- Bronze imutável e forensicamente auditável — qualquer registro que chegou pode ser inspecionado em sua forma original sem perda de informação.
- Pipeline continua mesmo quando a fonte muda formato, padrão de data, ou introduz valores inesperados — o ingest nunca falha por dados.
- Quarantine pattern (ADR-005) tem fonte de verdade para split: linhas rejected mantêm os tipos string-typed do Bronze, permitindo debug do valor original que causou a rejeição.
- Permite reprocessamento histórico: se a lógica de cast no Silver mudar, basta reprocessar do Bronze sem perder dados.
- Compatível com `delta.columnMapping.mode=name` no Bronze (ADR-009), permitindo evolução de schema sem rewrite de Parquet.

**Negativas:**

- Storage do Bronze é maior do que se fosse typed (strings ocupam mais bytes que tipos numéricos compactos). Para o volume deste case (~1700 rows totais), o impacto é desprezível; em produção com TB+, valeria avaliar.
- Consumidores diretos do Bronze precisam fazer casts. Mitigação: ninguém consome Bronze diretamente — ele alimenta apenas o Silver, que é a camada de consumo intermediária.
- Schema do Bronze não documenta semântica de tipos. Mitigação: `data_model.md` e o data dictionary `TABLES.md` documentam os tipos esperados por coluna.

**Trade-off aceito:** abrir mão de tipagem precoce em troca de resiliência operacional, auditabilidade forense e desacoplamento da evolução de schema da fonte.

## Notas de implementação

- ANSI mode é o padrão em Photon Spark 4.1 — nenhuma configuração extra é necessária. A proteção contra exceções é deslocada para a camada Silver via `try_*` family.
- `spark.databricks.delta.schema.autoMerge.enabled` NÃO é usada (Free Edition rejeita com CONFIG_NOT_AVAILABLE). Schema evolution no Bronze é controlada por `delta.columnMapping.mode=name` (ver ADR-009).
- A escolha de string-typed em Bronze é amplamente documentada como pattern Medallion canônico em referências de Delta Lake e Lakehouse architecture (Databricks Engineering Blog, "Building Medallion with Delta Lake", 2024-2025).

## Referências

- ADR-005 (DQ flags + quarantine — depende deste pattern)
- ADR-009 (schema evolution — complementa)
- `notebooks/01_bronze/01_bronze_ingest.py`
- `data/reference_databricks_free_edition.md` (memory) — gotchas de ANSI mode
