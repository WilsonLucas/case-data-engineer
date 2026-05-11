# Data Dictionary

> Catalogo completo das 29 tabelas do pipeline (9 bronze + 9 silver + 11 gold).
> Granularidade, PK, FKs, owner, SLA e link pra notebook fonte por tabela.
> Reflete o estado pos-aplicacao do `01_apply_governance.py` (Tier 1 do uplift).

**Owner default:** `wilsonlucas201@gmail.com` (em producao seria service principal `sp:pipeline-levva`)
**Catalog:** `workspace`

## Camada Landing

### `workspace.landing.sources` (UC Volume managed)
- **Tipo:** Volume (nao tabela)
- **Conteudo:** 9 arquivos brutos (CSV, JSON, NDJSON, XLSX, TXT pipe)
- **Path:** `/Volumes/workspace/landing/sources/`
- **Frequencia:** one-shot (case)
- **Notebook leitor:** [01_bronze_ingest.py](../notebooks/01_bronze/01_bronze_ingest.py)

## Camada Bronze (string-typed, ADR-001)

| Tabela | Granularidade | PK | Linhas | Source | Notebook |
|--------|---------------|-----|--------|--------|----------|
| `bronze.pedidos_cabecalho` | 1 linha por pedido | `order_id` | 403 | erp_pedidos_cabecalho_2025.csv | [bronze_ingest](../notebooks/01_bronze/01_bronze_ingest.py) |
| `bronze.pedidos_itens` | 1 linha por (pedido, item_seq) | `(order_id, item_seq)` | 995 | erp_pedidos_itens_2025.csv | [bronze_ingest](../notebooks/01_bronze/01_bronze_ingest.py) |
| `bronze.produtos` | 1 linha por produto | `product_code` (em product_json) | 72 | cadastro_produtos_api_dump.json | [bronze_ingest](../notebooks/01_bronze/01_bronze_ingest.py) |
| `bronze.clientes` | 1 linha por cliente | `customer_code` | 183 | crm_clientes_export.xlsx | [bronze_ingest](../notebooks/01_bronze/01_bronze_ingest.py) |
| `bronze.canais` | 1 linha por canal | `channel_code` | 8 | comercial_canais.xlsx | [bronze_ingest](../notebooks/01_bronze/01_bronze_ingest.py) |
| `bronze.regioes` | 1 linha por regiao | `regional_code` | 8 | legado_regioes_pipe.txt | [bronze_ingest](../notebooks/01_bronze/01_bronze_ingest.py) |
| `bronze.vendedores` | 1 linha por vendedor | `seller_id` | 42 | vendedores.csv | [bronze_ingest](../notebooks/01_bronze/01_bronze_ingest.py) |
| `bronze.entregas` | 1 linha por entrega | `delivery_id` | 322 | logistica_entregas.json | [bronze_ingest](../notebooks/01_bronze/01_bronze_ingest.py) |
| `bronze.ocorrencias` | 1 linha por ticket | `ticket_id` | 270 | atendimento_ocorrencias.ndjson | [bronze_ingest](../notebooks/01_bronze/01_bronze_ingest.py) |

**Caracteristicas Bronze:**
- Tipos: todos STRING (preserva formato original)
- Metadata: `_source_file`, `_ingestion_timestamp`, `_record_id`
- Tags UC: `layer=bronze`, `classification=internal`, `pii=true|false`, `data_domain={comercial|logistica|atendimento|transversal}`
- Idempotencia: `mode("overwrite") + overwriteSchema=true`
- SLA esperado em producao: <5min apos arquivo no Volume

## Camada Silver (typed + DQ + dedup)

| Tabela | Granularidade | PK | Linhas pos-dedup | Notebook |
|--------|---------------|-----|------------------|----------|
| `silver.pedidos_cabecalho` | 1 linha por pedido | `order_id` | 403 | [silver_pedidos](../notebooks/02_silver/02_silver_pedidos.py) |
| `silver.pedidos_itens` | 1 linha por item | `(order_id, item_seq)` | 995 | [silver_pedidos](../notebooks/02_silver/02_silver_pedidos.py) |
| `silver.produtos` | 1 linha por produto | `product_code` | 72 | [silver_produtos](../notebooks/02_silver/02_silver_produtos.py) |
| `silver.clientes` | 1 linha por cliente | `customer_code` | 180 (dedup 3) | [silver_clientes](../notebooks/02_silver/02_silver_clientes.py) |
| `silver.canais` | 1 linha por canal | `channel_code` | 7 (dedup CH05) | [silver_canais](../notebooks/02_silver/02_silver_canais.py) |
| `silver.regioes` | 1 linha por regiao | `regional_code` | 6 (dedup S/Sul, SE/Sudeste) | [silver_regioes](../notebooks/02_silver/02_silver_regioes.py) |
| `silver.vendedores` | 1 linha por vendedor | `seller_id` | 40 (dedup V004/V008) | [silver_vendedores](../notebooks/02_silver/02_silver_vendedores.py) |
| `silver.entregas` | 1 linha por entrega | `delivery_id` | 325 | [silver_entregas](../notebooks/02_silver/02_silver_entregas.py) |
| `silver.ocorrencias` | 1 linha por ticket | `ticket_id` | 270 | [silver_ocorrencias](../notebooks/02_silver/02_silver_ocorrencias.py) |

**Caracteristicas Silver:**
- Tipos: tipados (DECIMAL, DATE, TIMESTAMP) com casts ANSI-safe via `try_cast` em `F.expr`
- DQ: `_dq_status` (clean/warning/rejected) + `_dq_reasons` ARRAY (ADR-005)
- Dedup determinístico via window functions
- Quarantine isolado: registros `_dq_status='rejected'` separados em `silver.quarantine_*` (a implementar no Tier 2)
- Tags UC: `layer=silver` + 4 outras
- SLA esperado em producao: <10min apos Bronze

## Camada Gold (star schema dimensional)

### Dimensoes (6 SCD1 + 1 SCD2)

| Tabela | Granularidade | PK | SCD Type | Linhas | Notebook |
|--------|---------------|-----|----------|--------|----------|
| `gold.dim_cliente` | 1 linha por cliente | `customer_code` | Type 1 (overwrite) | 180 | [gold_dimensions](../notebooks/03_gold/03_gold_dimensions.py) |
| `gold.dim_cliente_history` | 1 linha por (cliente, versao) | `(customer_code, scd_hash)` | **Type 2** (MERGE) | 180 (1a ingest) | [gold_dimensions](../notebooks/03_gold/03_gold_dimensions.py) |
| `gold.dim_produto` | 1 linha por produto | `product_code` | Type 1 | 71 | [gold_dimensions](../notebooks/03_gold/03_gold_dimensions.py) |
| `gold.dim_canal` | 1 linha por canal | `channel_code` | Type 1 | 7 | [gold_dimensions](../notebooks/03_gold/03_gold_dimensions.py) |
| `gold.dim_regiao` | 1 linha por regiao | `regional_code` | Type 1 | 6 | [gold_dimensions](../notebooks/03_gold/03_gold_dimensions.py) |
| `gold.dim_vendedor` | 1 linha por vendedor | `seller_id` | Type 1 | 40 | [gold_dimensions](../notebooks/03_gold/03_gold_dimensions.py) |
| `gold.dim_data` | 1 linha por dia | `data_id` (yyyymmdd) | Type 1 | ~430 (range pedidos) | [gold_dimensions](../notebooks/03_gold/03_gold_dimensions.py) |

### Fatos (4)

| Tabela | Granularidade | PK | FKs | Linhas | Notebook |
|--------|---------------|-----|-----|--------|----------|
| `gold.fact_pedido` | 1 linha por pedido | `order_id` | `customer_code`, `seller_id`, `data_id`, `channel_code` | 403 | [gold_facts](../notebooks/03_gold/04_gold_facts.py) |
| `gold.fact_item` | 1 linha por item | `(order_id, item_seq)` | `product_code` | 995 | [gold_facts](../notebooks/03_gold/04_gold_facts.py) |
| `gold.fact_entrega` | 1 linha por entrega (parcial) | `delivery_id` | `order_id`, `data_envio_id`, `data_entrega_id` | 325 | [gold_facts](../notebooks/03_gold/04_gold_facts.py) |
| `gold.fact_ocorrencia` | 1 linha por ticket | `ticket_id` | `order_id`, `data_id` | 270 | [gold_facts](../notebooks/03_gold/04_gold_facts.py) |

### Views (1)

| View | Conteudo | Granularidade | Notebook |
|------|----------|---------------|----------|
| `gold.vw_kpi_business` | Pedidos pre-joined com cliente, canal, regiao, vendedor + flags operacionais | 1 linha por `order_id` | [gold_kpis](../notebooks/03_gold/05_gold_kpis.py) |

**Caracteristicas Gold:**
- TBLPROPERTIES Delta: `delta.logRetentionDuration=interval 30 days`, `delta.deletedFileRetentionDuration=interval 7 days`
- CHECK constraint em `fact_pedido`: `chk_net_amount_nonneg`
- Tags UC: `layer=gold` + 4 outras
- 100% das colunas business com COMMENT (gate REQ-002)
- View nao tem TAGS (limitacao Free Edition); COMMENT na view aponta para vw_kpi_business
- SLA esperado em producao: <15min apos Silver

## Reconciliacao (REQ-NF-002)

`SUM(net_amount) WHERE status_canonico IN ('FATURADO','EM_SEPARACAO')` = **R$ 1.707.675,84** em todas as 3 camadas (Bronze=Silver=Gold). Validado por `99_validation.py` Teste 2.

## Tags UC aplicadas (5 fixas)

Todas as 28 tabelas (excluindo view) tem 5 tags via `01_apply_governance.py`:

| Tag | Valor por tabela |
|-----|------------------|
| `owner` | `wilsonlucas201@gmail.com` |
| `layer` | `bronze` / `silver` / `gold` |
| `classification` | `internal` (uniforme no case) |
| `pii` | `true` (clientes, vendedores) / `false` (demais) |
| `data_domain` | `comercial` / `logistica` / `atendimento` / `calendario` / `transversal` |

Vocabulario controlado documentado em [NAMING_CONVENTIONS.md](NAMING_CONVENTIONS.md).

## Time Travel Delta (REQ-008)

Demonstrado em `99_validation.py` Teste 7:
- `DESCRIBE HISTORY workspace.gold.fact_pedido` lista versoes com `version`, `timestamp`, `operation`, `numOutputRows`
- `SELECT count(*) FROM workspace.gold.fact_pedido VERSION AS OF 0` retorna primeira versao
- Politica de retencao: 30 dias de log, 7 dias de arquivos deletados
