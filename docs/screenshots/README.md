# Evidências Visuais — Pipeline + Governança UC

Capturas de tela do workspace Databricks comprovando o pipeline rodando, o catálogo vivo aplicado, lineage automática Unity Catalog e reconciliação contábil. Servem como prova de execução real para a defesa oral.

## Inventário

| # | Arquivo | O que demonstra |
|---|---------|-----------------|
| 1 | `01-catalog-explorer-fact-entrega.png` | Tabela `gold.fact_entrega` no Catalog Explorer mostrando: Owner, 5 tags UC fixas (`classification:internal`, `data_domain:logistica`, `layer:gold`, `owner:wilsonlucas201@gmail.com`, `pii:false`), Description preenchido, e COMMENT em todas as colunas (PK da entrega, FKs para dim_data, etc). Prova visual do gate REQ-001 + REQ-002 + REQ-003 |
| 2 | `02-lineage-uc-fact-entrega.png` | Aba Lineage da `gold.fact_entrega` mostrando relacionamentos automaticamente capturados pelo Unity Catalog: upstream (`silver.entregas` Table + `04_gold_facts` Notebook) e downstream (`vw_kpi_business` View + `05_gold_kpis` Notebook + `99_validation` Notebook). Sem configuração manual — UC infere via Spark plan |
| 3 | `03-sample-data-genie.png` | Aba Sample Data da `gold.fact_entrega` com Genie integration (perguntas sugeridas em natural language: "What is the average delivery cost?", "Which carrier has the most deliveries?") + amostra dos dados reais (RODOVIÁRIO/AÉREO, atraso_dias, on_time_flag). Demonstra Free Edition + AI/ML capabilities |
| 4 | `04-pipeline-dag-success.png` | Job `case-levva-pipeline-end-to-end` com SUCCESS. DAG visual completo: bronze_ingest → 8 silvers em paralelo → gold_dimensions → gold_facts → gold_kpis → validation. 13 tasks verdes. Wall-clock 9m46s (gate REQ-NF-001 < 30min) |
| 5 | `05-vw-kpi-result-1707675.png` | SQL Editor executando query da seção 3.1 do BI_RUNBOOK com `valor_liquido` (nome correto da view). Resultado: receita_liquida_brl = **1.707.675,84**, qtd_pedidos = 229, ticket_medio = 7.457,10. Gate REQ-NF-002 visualmente confirmado (reconciliação Bronze=Silver=Gold mantida) |
| 6 | `06-catalog-schemas-overview.png` | Catalog Explorer raiz mostrando os 4 schemas com nomenclatura limpa pós Decisão 10: `bronze`, `gold`, `landing`, `silver` (sem prefixo redundante `case_levva_*`). Visualmente confirma o pattern Medallion canônico |

## Onde estão referenciados

- [`README.md`](../../README.md) — seção "Evidências visuais"
- [`docs/slides/case_levva.html`](../slides/case_levva.html) — slide "Evidências de execução"
