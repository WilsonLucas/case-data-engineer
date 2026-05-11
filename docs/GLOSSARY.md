# Glossario de Negocio

> Definicoes de termos de negocio e tecnicos usados no pipeline. Ponto unico de verdade para BI consumers e novos engenheiros entendendo o dominio.

## Termos de Negocio

### Pedido (`gold.fact_pedido`)
Transacao comercial entre cliente e empresa. Granularidade: 1 linha por `order_id`. Campos chave: `gross_amount`, `discount_amount`, `net_amount`, `status_canonico`, `payment_method`. Originado de `bronze.pedidos_cabecalho` (CSV `;` do ERP).

### Item de pedido (`gold.fact_item`)
Linha de detalhe de um pedido — produto + quantidade + preco unitario. Granularidade: 1 linha por `(order_id, item_seq)`. Permite analise por SKU. Originado de `bronze.pedidos_itens` (CSV `,` do ERP).

### Net amount
Valor liquido do pedido em BRL. `net_amount = gross_amount - discount_amount`. Validado no Silver via `divergencia_net_amount` flag (>0.01 marca DQ warning). Reconciliacao end-to-end Bronze=Silver=Gold = R$ 1.707.675,84 quando `status IN ('FATURADO', 'EM_SEPARACAO')`.

### Gross amount
Valor bruto do pedido antes de descontos. Em BRL.

### Discount amount
Desconto aplicado. Pode ser zero. Em BRL.

### Ticket medio
`SUM(net_amount) / COUNT(DISTINCT order_id)`. Calcular sobre `gold.vw_kpi_business` ou `gold.fact_pedido` filtrando pelo segmento de interesse (regiao, canal, periodo).

### Receita liquida
Soma de `net_amount` de pedidos `FATURADO` (excluindo `EM_SEPARACAO` e `CANCELADO`). Para receita reconhecida contabilmente. Para receita potencial (em pipeline), usar `FATURADO + EM_SEPARACAO`.

### Status canonico do pedido
Enum padronizado no Silver: `FATURADO`, `EM_SEPARACAO`, `CANCELADO`, `OUTRO`. Origem em `bronze.pedidos_cabecalho.status_order` que tem variantes (`EM_SEPARACAO`, `EM SEPARACAO`, `SEPARANDO`, `CANCELLED`). Mapeamento canonico em `02_silver_pedidos.py` celula de normalizacao.

### Payment method
Metodo de pagamento (CARTAO_CREDITO, BOLETO, PIX, etc). Extraido de `bronze.pedidos_cabecalho.payment_details` (JSON aninhado) no Silver. Coluna degenerate dimension em `fact_pedido` (atributo no fato sem dim propria por baixa cardinalidade).

### Taxa de cancelamento
`COUNT(WHERE status_canonico = 'CANCELADO') / COUNT(*)`. Calcular por janela temporal (mes, semana ISO) e segmento (canal, regiao). Indicador chave para analise de qualidade de venda.

### Taxa de atraso
`COUNT(WHERE eh_atrasado = true) / COUNT(*)` em `gold.fact_entrega`. `eh_atrasado` derivado de `delivered_at > promised_date`. Indicador operacional logistico.

### Lead time (entrega)
`delivered_at - shipped_at` em dias. Tempo entre saida do CD e chegada no cliente. Calculado em `gold.fact_entrega.lead_time_dias`. Quanto menor, melhor — gargalos aparecem em distribuicao.

### Severity (ocorrencia)
Severidade do ticket de atendimento. Enum: `LOW`, `MEDIUM`, `HIGH`. Default `MEDIUM` quando ausente. Mapeado para `severity_score` numerico (1-3) em `gold.fact_ocorrencia` para agregacoes ranqueadas.

### Event type (ocorrencia)
Tipo do evento que originou o ticket. Canonico: `REFUND`, `TROCA`, `DELAY`, `COMPLAINT`. Outros valores sao marcados em `_dq_reasons` mas mantidos no campo original.

### On-time delivery
`delivered_at <= promised_date`. Boolean. KPI principal de operacoes logisticas.

### Cliente
Pessoa fisica ou juridica que efetua compras. Granularidade: 1 linha por `customer_code`. Atributos chave: `cliente_segmento` (PEQUENO/MEDIO/GRANDE), `estado`, `cidade`, `status` (ATIVO/INATIVO). SCD2 disponivel em `gold.dim_cliente_history` para analise temporal de mudancas (ADR-002).

### Vendedor / Seller
Profissional responsavel pelo pedido. Granularidade: 1 linha por `seller_id`. Atributos: `nome`, `regional_code`, `status` (ATIVO/INATIVO), `hire_date`.

### Regiao
Subdivisao geografica do Brasil. Canonico: `NORTE`, `NORDESTE`, `CENTRO-OESTE`, `SUDESTE`, `SUL`. Originado de `bronze.regioes` (TXT pipe legado) com dedup determinístico (variantes `S`/`Sul`, `SE`/`Sudeste` colapsadas).

### Canal
Canal de venda (ECOMMERCE, LOJA_FISICA, MARKETPLACE, etc). 7 canais distintos pos-dedup (`CH05` duplicado removido). Granularidade: 1 linha por `channel_code`.

## Termos Tecnicos

### Bronze (camada)
Primeira camada do Medallion. Recebe dados brutos string-typed sem transformacao (ADR-001). Adiciona apenas metadata: `_source_file`, `_ingestion_timestamp`, `_record_id`. Imutavel. 9 tabelas + `pipeline_metrics`.

### Silver (camada)
Camada intermediaria. Aplica casts, dedup determinístico (window functions com `row_number`), parse de JSON aninhado, padronizacao de enums. Marca DQ flags inline (`_dq_status` + `_dq_reasons`) e isola rejected em `quarantine_<entity>` (ADR-005). Alimenta o Gold.

### Gold (camada)
Camada analitica em star schema. 6 dimensoes SCD1 + 1 SCD2 demonstrativa (`dim_cliente_history`) + 4 facts + 1 view consolidada (`vw_kpi_business`). Pronta para consumo BI.

### DQ status
`_dq_status` ENUM: `clean` (sem issues), `warning` (formato/enum nao canonico), `rejected` (PK/FK/null obrigatorio ausente — vai para quarantine). Classificacao por TIPO, nao por contagem (ADR-005, REQ-DQ-002).

### DQ reasons
`_dq_reasons` ARRAY<STRING> com razoes legiveis em PT-BR (`"email invalido"`, `"order_date inválida"`). Construido via `F.array_remove(F.array(F.when(...).otherwise(F.lit(None)), ...), None)`.

### Quarantine
Tabela paralela `silver.quarantine_<entity>` com registros `_dq_status='rejected'` isolados. Schema = colunas originais string-typed do bronze + 4 metadados (`_quarantine_timestamp`, `_quarantine_reason`, `_source_file`, `_ingestion_timestamp`). Preserva valor original para debug forense (ADR-005).

### SCD1 (Slowly Changing Dimension Type 1)
Dimensao que sempre reflete o estado atual. Mudancas sobrescrevem valores antigos via `mode("overwrite")`. Padrao default em todas as 6 dims (exceto `dim_cliente_history`). Documentado em `data_model.md` e ADR-002.

### SCD2 (Slowly Changing Dimension Type 2)
Dimensao com historico versionado. Cada mudanca gera nova linha com `effective_date`/`end_date`/`is_current`. Implementada em `gold.dim_cliente_history` via Delta `MERGE INTO` com hash MD5 sobre 4 colunas tracking (`segmento`, `estado`, `cidade`, `status`). ADR-002 documenta o design.

### Idempotencia
Propriedade de operacoes que produzem o mesmo resultado quando re-executadas. Pipeline garante via `mode("overwrite") + overwriteSchema=true` em todos os writes Delta. CHECK constraints idempotentes via `DROP CONSTRAINT IF EXISTS` antes de cada `ADD CONSTRAINT`.

### Time travel (Delta Lake)
Capacidade de consultar versoes historicas de uma tabela Delta via `VERSION AS OF <N>` ou `TIMESTAMP AS OF '<ts>'`. Demonstrado em `99_validation.py` Teste 7. Politica de retencao: `delta.logRetentionDuration = interval 30 days`.

### Pipeline metrics
Tabela append-only `bronze.pipeline_metrics` com observabilidade por task: `task_name`, `row_count`, `duration_seconds`, `status`, `_ts`. Permite query `SELECT task_name, AVG(duration_seconds) FROM pipeline_metrics GROUP BY task_name`.

### Reconciliacao
Verificacao de que numeros batem entre camadas. Gate principal: `SUM(net_amount) WHERE status IN ('FATURADO','EM_SEPARACAO')` = R$ 1.707.675,84 em Bronze, Silver e Gold (REQ-NF-002). Validado em cada execucao do `99_validation.py`.

### ANSI mode
Modo SQL estrito ativado por padrao em Photon Spark 4.1. Casts falhos lancam excecao ao inves de retornar NULL. Mitigado no projeto via `try_cast`, `try_to_date`, `try_to_timestamp` chamados via `F.expr()` (funcoes nao expostas como wrappers Python no Free Edition).

### Unity Catalog (UC)
Sistema de governanca de dados do Databricks. Hierarquia 3 niveis: `catalog.schema.table`. Suporta COMMENT, TAGS, FK informational, CHECK constraints, lineage automatica. Usado neste projeto para catalogo vivo (ADR-003).

### Unity Catalog Volume
Storage managed para arquivos brutos (substitui DBFS legado). Path: `/Volumes/<catalog>/<schema>/<volume_name>/`. Aqui: `/Volumes/workspace/landing/sources/`.
