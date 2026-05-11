# Resumo Executivo - Case Engenheiro de Dados

> Sintese tecnica em 2 paginas. Para detalhes, ver `README.md` e pasta `docs/`.

---

## O que foi construido

Solucao end-to-end de engenharia de dados sobre **Databricks Free Edition com Unity Catalog** que transforma 9 fontes brutas heterogeneas (CSV `;`, CSV `,`, JSON aninhado, NDJSON, Pipe TXT, XLSX) em um **modelo dimensional analitico** consumivel por um Analista de BI.

A solucao adota arquitetura **Medallion (Bronze, Silver, Gold)** com **Delta Lake** como formato de armazenamento gerenciado pelo Unity Catalog, **PySpark** para transformacoes e **Spark SQL** para a camada analitica final. Orquestracao via **multi-task job** com DAG explicito de dependencias e paralelismo dos silvers.

### Volumes processados (numeros reais pos-execucao)

| Entidade | Bronze | Silver | Gold (dim/fact) |
|---|---|---|---|
| pedidos_cabecalho | 403 | 403 | 403 (`fact_pedido`) |
| pedidos_itens | 995 | 995 | 995 (`fact_item`) |
| produtos | 72 | 72 | 71 (`dim_produto`, 1 filtrado por null) |
| clientes | 183 | 180 (dedup 3 duplicatas) | 180 (`dim_cliente`) |
| canais | 8 | 7 (dedup CH05 conflitante) | 7 (`dim_canal`) |
| vendedores | 42 | 40 (dedup V004/V008) | 40 (`dim_vendedor`) |
| regioes | 8 | 6 (dedup S/Sul + SE/Sudeste) | 6 (`dim_regiao`) |
| ocorrencias | 270 | 270 | 270 (`fact_ocorrencia`) |
| entregas | 322 | 325 | 325 (`fact_entrega`) |

**`dim_data` gerada**: 493 dias cobrindo o range temporal dos fatos.
**`vw_kpi_business`**: 403 linhas (1 por pedido, granular pedido).

### Reconciliacao Bronze -> Silver -> Gold

`SUM(net_amount) WHERE status IN ('FATURADO', 'EM_SEPARACAO')`:

| Camada | Valor |
|---|---|
| Bronze | R$ 1.707.675,84 |
| Silver | R$ 1.707.675,84 |
| Gold (`fact_pedido`) | R$ 1.707.675,84 |

Todos batem: zero divergencia ponta a ponta.

### Storage e namespace

- **Sources**: UC Volume `/Volumes/workspace/case_levva/sources/`
- **Bronze**: `workspace.case_levva_bronze.*`
- **Silver**: `workspace.case_levva_silver.*`
- **Gold**: `workspace.case_levva_gold.*`

---

## Principais decisoes tecnicas

| Decisao | Por que |
|---|---|
| **Bronze como string-typed** | Preserva qualquer formato original; cast so no Silver com `try_cast` via `F.expr` para resiliencia contra dados ruins e ANSI mode estrito do Photon Spark 4.1 |
| **Padrao Medallion completo** | Separacao clara de responsabilidades: ingestao (Bronze), tratamento (Silver), modelagem (Gold) |
| **Unity Catalog namespaced por camada** | `workspace.case_levva_bronze/silver/gold` evita colisao em ambientes multi-projeto e facilita GRANTs |
| **DQ flags ao inves de descarte** | Registros problematicos sao marcados com `_dq_status` (clean/warning/rejected) e `_dq_reasons` (array de razoes), nao removidos |
| **`dim_data` gerada** | Garante cobertura temporal completa, suporta analise por dia da semana ou trimestre sem calculos runtime |
| **View `vw_kpi_business` pre-joinada** | Granularidade pedido com tudo enriquecido; Analista de BI consulta direto sem fazer joins manuais |
| **`mode("overwrite")` idempotente** | Todos os notebooks rodam de novo sem corromper estado |
| **Lookup canonico para enums** | Variacoes de caps (Faturado/faturado/EM_SEPARACAO) padronizadas para 4 valores oficiais |
| **Multi-task job com DAG** | Orquestra paralelismo dos silvers e dependencias explicitas; vitrine visual no UI do Databricks |

---

## Principais desafios encontrados

### Heterogeneidade de formatos
6 formatos diferentes nos sources, incluindo XLSX (que nao e nativo do Spark, foi necessario `pandas.read_excel`) e JSON profundamente aninhado (entregas tem `carrier{}`, `timestamps{}`, `destination{}`). Resolvido com readers especificos por formato e flatten via dot notation no Silver.

### Inconsistencia de qualidade
- **3 formatos de data** coexistindo na mesma coluna (ISO, BR, BR-com-hora) resolvido com `coalesce(try_to_date)` em ordem via `F.expr`
- **Duplicatas reais** em vendedores (V004, V008 aparecem 2x) resolvidas com `row_number() over (partition by seller_id order by hire_date desc, _record_id desc)`
- **Duplicata conflitante** CH05 em canais (`E-commerce` vs `ecommerce` com tipo diferente) resolvida priorizando registro com `observacao` null
- **Inconsistencia de regional_code** (`S` e `sul` referenciam mesma regiao) resolvida com tabela canonica de mapeamento
- **18 variantes de `estado`** em clientes (UF, nome cheio, typo `Sta Catarina`) resolvidas com `UF_MAP` exaustivo
- **Decimal BR vs US** (virgula vs ponto) resolvido com `regexp_replace(',', '.')` antes do cast
- **3 customer_id duplicados** em clientes (`c0051` minusculo) resolvidos com UPPER + dedup por `updated_at`

### ANSI mode estrito no Photon Spark 4.1
Free Edition tem ANSI SQL mode default ativado, que rejeita operacoes que historicamente retornavam NULL. Tratamentos especificos:
- `cast("int")` em `"5.0"` -> `cast("double").cast("int")`
- `cast("decimal")` em `"N/A"` -> `try_cast` via `F.expr`
- `to_date()` falho -> `try_to_date` via `F.expr`
- `'T'` literal em pattern timestamp ISO -> `replace(col, 'T', ' ')` + parse `yyyy-MM-dd HH:mm:ss`

### Limitacoes do Free Edition
- Serverless only, sem cluster proprio para tuning fino
- Concorrencia limitada de tasks paralelas (silvers serializam quando estouram quota)
- `spark.databricks.delta.schema.autoMerge.enabled` nao disponivel; removido

---

## Visao geral do modelo final

### 6 dimensoes + 4 fatos + 1 view

```
Dimensoes (SCD Type 1)         Fatos
---------------------          ----------------
dim_cliente                    fact_pedido (granularidade pedido)
dim_produto                    fact_item (granularidade item)
dim_canal                      fact_entrega (granularidade entrega)
dim_regiao                     fact_ocorrencia (granularidade ticket)
dim_vendedor
dim_data (gerada)              vw_kpi_business (consolidada)
```

### Metricas que o BI consegue calcular direto

- **Receita liquida** (`SUM(net_amount)`) por qualquer dimensao
- **Quantidade de pedidos** (`COUNT`) por qualquer dimensao
- **Ticket medio** (`AVG(net_amount)`)
- **Taxa de cancelamento** (em valor e em pedidos)
- **Taxa de atraso** (entregas com `delivered_at > promised_date`)
- **Tempo medio de entrega** (`AVG(lead_time_dias)`)
- **Volume e severidade** de ocorrencias por tipo

---

## Proximos passos recomendados para evolucao

### Curto prazo (1-2 semanas)
1. **Migracao para Databricks Premium** - controle de cluster, RBAC, audit logs
2. **Orquestracao via Workflows agendados** - substituir `jobs submit` on-demand por job declarativo recorrente
3. **Adicionar testes automatizados** (`pytest-spark` ou `chispa`) - coverage minima nos transforms criticos

### Medio prazo (1-2 meses)
4. **CDC nas fontes transacionais** - ERP de pedidos provavelmente ja tem CDC; ingestao incremental real ao inves de full refresh
5. **Delta Live Tables** - substitui DQ manual por expectations declarativas
6. **Particionamento estrategico** dos fatos por `ano_mes` + `regional_code` para query performance
7. **Observabilidade** - exportar metricas de DQ + volumes processados para Datadog ou Grafana

### Longo prazo (trimestre)
8. **CI/CD via Databricks Asset Bundles** - deploy automatizado dev -> stage -> prod
9. **Catalogo semantico** (dbt + Atlan/DataHub) - documentacao viva do modelo dimensional
10. **Self-service** - habilitar Analistas a consumirem via SQL Warehouse + acesso controlado por role

---

## Estrutura entregue

```
case-data-engineer-levva/
|-- README.md                      # Documentacao principal + diagrama Mermaid
|-- EXECUTIVE_SUMMARY.md           # Este documento
|-- docs/
|   |-- architecture.md            # Detalhe tecnico das camadas
|   |-- data_quality.md            # Issues encontradas + tratamentos (51 issues mapeadas)
|   |-- data_model.md              # Star schema completo + ER diagram
|   `-- business_questions.md      # 5 perguntas do PDF respondidas com SQL
|-- notebooks/
|   |-- 00_setup/                  # Exploracao + validacao end-to-end
|   |   |-- 00_exploration.py
|   |   `-- 99_validation.py
|   |-- 01_bronze/
|   |   `-- 01_bronze_ingest.py    # Ingestao multi-formato
|   |-- 02_silver/                 # 8 notebooks por entidade
|   |   `-- 02_silver_*.py
|   `-- 03_gold/
|       |-- 03_gold_dimensions.py  # 6 dimensoes
|       |-- 04_gold_facts.py       # 4 fatos
|       `-- 05_gold_kpis.py        # View consolidada
`-- diagrams/
    `-- architecture.mmd
```

**Tempo de execucao end-to-end:** ~15-25 minutos em serverless do Free Edition (cold start + fila de concorrencia).

**Repositorio publico:** https://github.com/WilsonLucas/case-data-engineer
