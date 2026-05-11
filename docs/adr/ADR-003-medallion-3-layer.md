# ADR-003: Arquitetura Medallion em 3 camadas com schemas namespaced

## Status

Aceito — 2026-05-11.

## Contexto

O case requer transformar 9 fontes brutas em uma base analítica organizada para consumo por Analista de BI. As fontes têm formatos heterogêneos (CSV, JSON aninhado, NDJSON, XLSX, TXT pipe), padrões de qualidade variáveis e relacionamentos implícitos.

Três arquiteturas relevantes foram avaliadas:

A. **Single-layer**: ler fonte → transformar → escrever tabela final em uma única passada por entidade. Simples mas sem isolamento de etapas; debug difícil; reprocessamento custoso.

B. **Two-layer (raw → mart)**: separar staging (cópia da fonte) de mart (modelo final). Melhor que single-layer mas perde camada de transformação intermediária; queries de DQ ficam misturadas com queries de modelagem.

C. **Three-layer (Medallion: Bronze / Silver / Gold)**: pattern canônico Databricks. Bronze recebe a fonte sem transformação; Silver aplica casts, dedup e DQ; Gold modela em estrela para consumo.

A escolha tem implicações em: organização do catálogo Unity Catalog, granularidade de owner/SLA por camada, política de retenção, idempotência de cada etapa e clareza para o consumidor BI.

## Decisão

Adotar **Medallion 3-camadas + landing** com **schemas curtos** (sem prefixo de projeto) dentro do catálogo `workspace`:

```
workspace                       (catalog default Free Edition)
├── landing                     (schema-pai — só hospeda o Volume; sem tabelas)
│   └── sources                 (UC Volume managed para arquivos brutos)
└── bronze                      (camada Bronze — string-typed, ADR-001)
    ├── pedidos_cabecalho
    ├── pedidos_itens
    ├── produtos
    ├── clientes
    ├── canais
    ├── regioes
    ├── vendedores
    ├── ocorrencias
    ├── entregas
    └── pipeline_metrics        (observabilidade append-only)
└── silver                      (camada Silver — typed, dedup, DQ split)
    ├── pedidos_cabecalho
    ├── pedidos_itens
    ├── produtos
    ├── clientes
    ├── canais
    ├── regioes
    ├── vendedores
    ├── ocorrencias
    ├── entregas
    └── quarantine_*            (tabelas de DQ rejected isolado, ADR-005)
└── gold                        (camada Gold — star schema)
    ├── dim_cliente             (SCD1)
    ├── dim_cliente_history     (SCD2 demonstrativa, ADR-002)
    ├── dim_produto, dim_canal, dim_regiao, dim_vendedor, dim_data
    ├── fact_pedido, fact_item, fact_entrega, fact_ocorrencia
    └── vw_kpi_business         (view consolidada para BI)
```

**Nota sobre nomenclatura:** uma versão anterior usava prefixo `case_levva_` em todos os schemas (`case_levva_bronze`, `case_levva_silver`, etc) e o schema-pai chamado `case_levva`. Foi refatorado para usar nomes curtos canônicos durante o uplift 200% — o catálogo `workspace` já delimita escopo no Free Edition single-tenant, e o pattern Medallion canônico (Databricks Engineering Blog) usa `bronze`/`silver`/`gold` puros. Schema do Volume foi renomeado para `landing` (termo canônico em arquitetura ETL para "área onde arquivos chegam antes de processados"). Path do Volume: `/Volumes/workspace/landing/sources/`.

### Responsabilidades por camada

| Camada | Responsabilidade | Tipo de operação | Idempotência |
|--------|-------------------|-------------------|---------------|
| **Bronze** | Cópia 1:1 da fonte preservando bytes originais; adiciona apenas `_source_file`, `_ingestion_timestamp`, `_record_id` | `read → write` (sem transformação de conteúdo) | `mode("overwrite") + overwriteSchema=true` |
| **Silver** | Cast/parse com ANSI handling; dedup determinístico via window functions; split DQ rejected→quarantine, clean+warning→silver; padronização de enums | `read bronze → transformar → split → write silver + write quarantine` | Idem |
| **Gold** | Modelagem em estrela (dimensões + fatos); SCD1 padrão + SCD2 onde justificado; CHECK constraints; FK informational; view consolidada para BI | `read silver → join → agregar → write gold` | Idem (com exceção de `dim_cliente_history` que usa MERGE INTO Delta) |

### Por que schemas separados (não sub-schemas hierárquicos)

UC permite hierarquia `catalog.schema.table` (3 níveis), não `catalog.schema.subschema.table`. Para representar 3 camadas, há 2 opções:
- **A. Um schema único com prefixos** (`bronze_pedidos`, `silver_pedidos`, `gold_dim_cliente` em um só schema).
- **B. Schemas separados por camada** (`bronze.pedidos`, `silver.pedidos`, `gold.dim_cliente`).

Adotamos B porque:
- Filtros no Catalog Explorer ficam mais limpos: `SHOW TABLES IN silver` lista exatamente Silver.
- Permite aplicar GRANT/REVOKE por camada em produção (Bronze restrito ao time de engenharia, Gold disponível ao BI).
- Tags `layer` ficam ortogonais ao schema name — possibilita filtro cruzado em produção.
- Queries SQL mais curtas no `BI_RUNBOOK.md`: `SELECT * FROM gold.fact_pedido` vs `SELECT * FROM case_levva_gold.fact_pedido`.

## Alternativas rejeitadas

1. **Single-layer**: rejeitado — sem isolamento; debug de DQ misturado com bug de modelagem; reprocessar 1 dimensão exige re-rodar tudo.

2. **Two-layer (raw + mart)**: rejeitado — sem camada intermediária para DQ + dedup separadamente da modelagem; mart fica bagunçada.

3. **Schema único com prefixos** (`case_levva.bronze_pedidos`, etc): rejeitado — Catalog Explorer fica longo demais; sem possibilidade de RBAC por camada em produção.

4. **4-camadas (adicionar Platinum)**: rejeitado — escopo do case não justifica camada de agregações pré-calculadas; `vw_kpi_business` resolve.

## Consequências

**Positivas:**

- Cada camada tem responsabilidade única e testável independentemente.
- Reprocessamento granular: dim_cliente quebra → re-rodar `gold_dimensions` apenas, sem refazer Bronze ou Silver.
- DAG do pipeline reflete a arquitetura: 1 task Bronze + 8 tasks Silver paralelas + 3 tasks Gold + validation + governance (ver `pipeline_dag.json`).
- Catálogo organizado por camada facilita exploração pelo avaliador no Catalog Explorer.
- `pipeline_metrics` em `case_levva_bronze` (não em schema separado) mantém observability vinculada ao primeiro schema do projeto.

**Negativas:**

- 3 schemas a criar e gerenciar — mais DDL inicial. Mitigado por script de setup único.
- Volume de tabelas total (9 Bronze + 8 Silver + 4-8 quarantine + 11 Gold = 32+ tabelas) é grande para um case. Aceito porque é exatamente o ponto demonstrativo: organização escalável.
- Consumidores precisam saber qual camada ler — Bronze nunca, Silver para análise técnica, Gold para BI. Mitigado pela documentação `BI_RUNBOOK.md`.

**Trade-off aceito:** mais DDL e organização inicial > pipeline plano mas confuso.

## Referências

- ADR-001 (Bronze string-typed — define a camada Bronze)
- ADR-005 (DQ flags + quarantine — define como Silver split funciona)
- ADR-007 (retenção Delta — política aplicada por camada)
- `pipeline_dag.json` — DAG reflete a arquitetura de 3 camadas
- `docs/data_model.md` — modelagem da camada Gold
- Databricks Engineering Blog, "Building Medallion Architectures with Delta Lake" (referência canônica)
