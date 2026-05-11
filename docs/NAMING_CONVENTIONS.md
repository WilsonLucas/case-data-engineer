# Convencoes de Nomenclatura

> Documento de referencia para o naming usado em todo o pipeline. Aplicavel a schemas, tabelas, colunas, tags Unity Catalog, ADRs e arquivos de configuracao.

## 1. Schemas (Unity Catalog)

Catalog unico: `workspace` (default do Free Edition; ADR-004).

| Schema | Proposito | Tabelas |
|--------|-----------|---------|
| `workspace.landing` | Hospeda Volume `sources` com arquivos brutos | (sem tabelas) |
| `workspace.bronze` | Camada Bronze string-typed (ADR-001) | 9 tabelas + `pipeline_metrics` |
| `workspace.silver` | Camada Silver tipada com DQ flags + quarantine | 9 tabelas + `quarantine_*` |
| `workspace.gold` | Camada Gold star schema | 6 dims + 1 SCD2 + 4 facts + 1 view |

Schemas curtos sem prefixo de projeto. Catalog `workspace` ja delimita escopo (single-tenant Free Edition). ADR-003 documenta a escolha.

## 2. Tabelas

| Categoria | Padrao | Exemplo |
|-----------|--------|---------|
| Bronze | nome igual a entidade de negocio (singular ou plural conforme fonte) | `bronze.pedidos_cabecalho`, `bronze.clientes` |
| Silver | mesmo nome do bronze | `silver.pedidos_cabecalho`, `silver.clientes` |
| Silver quarantine | `quarantine_<entity>` | `silver.quarantine_clientes` |
| Gold dimensoes | prefixo `dim_` | `gold.dim_cliente`, `gold.dim_data` |
| Gold dimensoes SCD2 | sufixo `_history` | `gold.dim_cliente_history` |
| Gold fatos | prefixo `fact_` | `gold.fact_pedido`, `gold.fact_entrega` |
| Gold views | prefixo `vw_` | `gold.vw_kpi_business` |
| Observability | sufixo `_metrics` | `bronze.pipeline_metrics` |

Tudo `snake_case`. Nomes em portugues onde refletem entidade de negocio (`pedido`, `cliente`, `entrega`); nomes tecnicos em ingles (`metrics`, `quarantine`, `history`).

## 3. Colunas

| Tipo | Padrao | Exemplo |
|------|--------|---------|
| Chave primaria de negocio | `<entity>_id` ou `<entity>_code` | `order_id`, `customer_code` |
| Chave estrangeira | mesmo nome da PK referenciada | `customer_code` em `fact_pedido` aponta para `dim_cliente.customer_code` |
| Data | `<descricao>_date` | `order_date`, `effective_date` |
| Timestamp | `<descricao>_at` ou `<descricao>_ts` | `created_at`, `_ingestion_timestamp` |
| Valor monetario | `<descricao>_amount` | `gross_amount`, `net_amount`, `discount_amount` |
| Quantidade | `qty_<descricao>` ou `<entity>_count` | `qty_itens`, `vw_count` |
| Flag boolean | `eh_<descricao>` ou `is_<descricao>` (PT-BR para BI) | `eh_feriado`, `is_current`, `eh_dia_util` |
| Coluna tecnica (interna) | prefixo `_` | `_dq_status`, `_dq_reasons`, `_source_file`, `_ingestion_timestamp`, `_record_id`, `_quarantine_timestamp`, `_quarantine_reason` |
| Coluna canonica derivada | sufixo `_canonico` | `status_canonico` |

Colunas tecnicas com prefixo `_` ficam sempre no final do schema. Sao excluidas de critérios de aceite que medem cobertura de COMMENT em colunas de negocio (REQ-002).

## 4. Tags Unity Catalog (5 fixas)

Aplicadas em todas as 19 tabelas via script `01_apply_governance.py`. Documentadas em ADR-008 (vocabulario controlado).

| Tag | Valores Validos | Proposito |
|-----|------------------|-----------|
| `owner` | email valido (ex: `wilsonlucas201@gmail.com`) | Responsavel pela tabela. Em producao, seria service principal ou team handle |
| `layer` | `bronze`, `silver`, `gold` | Camada Medallion (ADR-003) |
| `classification` | `internal`, `confidential`, `public` | Classificacao geral de acesso |
| `pii` | `true`, `false` | Boolean ortogonal: tabela contem dados pessoais identificaveis? |
| `data_domain` | `comercial`, `logistica`, `atendimento`, `calendario`, `transversal` | Dominio funcional para descoberta no Catalog Explorer |

Exemplo de aplicacao:
```sql
ALTER TABLE workspace.gold.dim_cliente
SET TAGS (
    'owner' = 'wilsonlucas201@gmail.com',
    'layer' = 'gold',
    'classification' = 'internal',
    'pii' = 'true',
    'data_domain' = 'comercial'
);
```

## 5. ADRs

| Padrao | Localizacao | Exemplo |
|--------|-------------|---------|
| Numeracao | sequencial 3 digitos | `ADR-001`, `ADR-002`, ..., `ADR-099` |
| Filename | `ADR-NNN-<topico-curto>.md` (kebab-case) | `ADR-001-bronze-as-string.md` |
| Localizacao | `docs/adr/` | `docs/adr/ADR-005-dq-flags-vs-dlq.md` |
| Estrutura | Template Michael Nygard PT-BR | 4 secoes obrigatorias: `## Status`, `## Contexto`, `## Decisao`, `## Consequencias` |
| Tamanho minimo | 150 palavras (sem template vazio) | — |

## 6. DQ flags

| Coluna | Tipo | Valores |
|--------|------|---------|
| `_dq_status` | STRING NOT NULL | `clean`, `warning`, `rejected` |
| `_dq_reasons` | ARRAY<STRING> | Lista de razoes legiveis em PT-BR |

Classificacao por TIPO (ADR-005, REQ-DQ-002): PK/FK/null obrigatorio = rejected; formato/enum = warning; sem issues = clean. NUNCA por contagem (`size <= 2`).

Quarantine schema = colunas originais bronze + `_quarantine_timestamp`, `_quarantine_reason`, `_source_file`, `_ingestion_timestamp`.

## 7. Notebooks

| Padrao | Exemplo |
|--------|---------|
| Filename | `<NN>_<descricao>.py` numerado por ordem de execucao | `01_bronze_ingest.py`, `02_silver_pedidos.py`, `99_validation.py` |
| Estrutura de pastas | `notebooks/<NN>_<camada>/` | `notebooks/00_setup/`, `notebooks/01_bronze/` |
| Cabecalho Dataside | Primeira celula `%md` com `# Tabela:` ou `# Notebook:`, `## Objetivo:`, `## Fontes de Dados`, `## Historico de alteracoes` | Ver qualquer notebook do projeto |
| Imports | `from pyspark.sql import functions as F` (alias F obrigatorio); imports explicitos de tipos (NAO `import *`) | — |

## 8. Codigo de vitrine (publico)

Aplicavel a TODOS os arquivos do repo publico:

- ZERO emojis em codigo `.py`, diagramas `.mmd`, ou strings de print/raise.
- Caracteres ASCII puros em mensagens de log; acentos OK em comentarios markdown.
- Black com `line-length = 120` (configurado em `pyproject.toml`).
- Ruff com selecao `E, F, W, I` (configurado em `pyproject.toml`).
