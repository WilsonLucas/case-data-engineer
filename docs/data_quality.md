# Qualidade de Dados - Diagnóstico e Tratamentos

> Inventário dos problemas encontrados nas fontes brutas e decisões aplicadas em cada um.

---

## Princípio orientador

**Não descartar registros silenciosamente.** Toda decisão de tratamento é documentada e o registro problemático é marcado (não removido), permitindo ao Analista de BI:

1. Ver volume de dados afetados
2. Decidir excluir ou não no dashboard
3. Reportar para corrigir na origem

Cada tabela Silver carrega:
- `_dq_status` - `clean` / `warning` / `rejected`
- `_dq_reasons` - array de strings descrevendo issues encontradas

---

## Issues por fonte

### erp_pedidos_cabecalho_2025.csv

| # | Issue | Volume estimado | Tratamento aplicado | Decisão |
|---|-------|----------------|---------------------|---------|
| 1 | 3 formatos de data (`order_date`, `promised_date`, `last_update`): `yyyy-MM-dd`, `dd/MM/yyyy`, `dd/MM/yyyy HH:mm` | ~30% mistos | `coalesce(try_to_date)` em ordem de tentativa | Mantém |
| 2 | `status_order` em caps mista: `Faturado`, `faturado`, `EM_SEPARACAO`, `cancelado` | 100% | `UPPER(status_order)` + lookup canônico | Mantém |
| 3 | Decimal BR em `gross_amount`, `discount_amount`, `net_amount` (vírgula como decimal) | 100% | `regexp_replace(',', '.')` + `cast(decimal(15,2))` | Mantém |
| 4 | `payment_details` é JSON string aninhada | 100% | `from_json` com schema explícito -> colunas separadas | Mantém |
| 5 | Possíveis order_id duplicados | A confirmar no exploration | `row_number() over (partition by order_id order by _ingestion_timestamp desc)` mantém o mais recente | Mantém |

### erp_pedidos_itens_2025.csv

| # | Issue | Volume | Tratamento | Decisão |
|---|-------|--------|------------|---------|
| 6 | `item_status` vazio | ~20% | `coalesce(item_status, 'NAO_INFORMADO')` + flag DQ | Mantém com flag `warning` |
| 7 | `total_item` não bate com `quantity * unit_price` (arredondamento) | ? | Recalcular `total_item_calculado = quantity * unit_price` no Silver e comparar | Mantém ambos (original + calculado), flag se diff > 0.01 |

### cadastro_produtos_api_dump.json

| # | Issue | Volume | Tratamento | Decisão |
|---|-------|--------|------------|---------|
| 8 | Estrutura aninhada (`product`, `pricing`, `attributes` como objects) | 100% | Flatten via `select(col("product.product_id").alias("product_code"), ...)` | Mantém |
| 9 | `status` em caps mista: `Ativo`, `ativo` | ~50% | `UPPER(status) IN ('ATIVO', 'ATIVO ')` -> `is_active` boolean | Normaliza para boolean |
| 10 | `pricing.currency` sempre BRL? | 100% (a confirmar) | Validar e assumir BRL como default | Mantém |
| 11 | `attributes.tags` é array | 100% | Mantém como array no Silver; explode só quando necessário no Gold | Mantém |

### crm_clientes_export.xlsx

**Schema real:** 183 linhas | 10 colunas | sheet `Sheet1` | PK = `customer_id`.

| # | Issue | Volume | Tratamento | Decisão |
|---|-------|--------|------------|---------|
| 12 | **3 duplicatas reais**: 183 linhas vs 180 customer_id únicos | ~1.6% | Dedup por `customer_id` ordenado por `updated_at DESC, _record_id DESC` | Mantém o mais recente |
| 13 | `customer_id` em lowercase: `c0051` (resto `C00xx`) | 1 registro | `UPPER(TRIM(customer_id))` | Normaliza para `C0051`, junta com a duplicata |
| 14 | `data_cadastro` em **3 formatos misturados**: `2024-01-26`, `2025/09/08`, `18/12/2024` | 100% | `coalesce(to_date(fmt))` em ordem; null se nenhum casa | Mantém + warning se null |
| 15 | `status_cliente` com 5 variantes: `Ativo`/`ATIVO`/`ativo`/`Inativo`/`inativo` | 100% | `UPPER(TRIM)` -> `ATIVO`/`INATIVO` | Padroniza |
| 16 | `status_cliente` null | 19.1% (35 registros) | Mantém null + warning DQ (não inferimos status) | Mantém com warning |
| 17 | `porte` caps mista: `Grande` vs `grande` | 100% das ocorrências de "grande" | `UPPER(TRIM)` | Padroniza |
| 18 | `porte` null | 21.3% (39 registros) | Mantém null + warning DQ | Mantém com warning |
| 19 | `segmento` null | 18.6% (34 registros) | Mantém null + warning DQ | Mantém com warning |
| 20 | `estado` com 18 valores misturando UF, nome cheio, typo (`Sta Catarina`) | 100% | Lookup map exaustivo `UF_MAP` -> coluna `uf` 2 letras. `estado_original` preservado para auditoria | Padroniza para UF; quem não casa fica null + warning |
| 21 | `email` inválido (sem `@`): `duplicado_sem_arroba.com` no registro `c0051` | ~0.5% | Regex `.+@.+\..+` -> coluna `email_valid` boolean. **DQ status = rejected** se inválido | Marca como rejected |
| 22 | `email` null | 2.2% (4 registros) | Idem #21 - null falha o regex | Marca como rejected |

### comercial_canais.xlsx

**Schema real:** 8 linhas | 5 colunas | **sheet `canais`** (NÃO `Sheet1` - atenção no `pd.read_excel`) | PK = `id_canal`.

| # | Issue | Volume | Tratamento | Decisão |
|---|-------|--------|------------|---------|
| 23 | **Duplicata conflitante CH05**: 2 linhas com `nome_canal` divergente (`E-commerce` vs `ecommerce`) e `tipo_canal` divergente (`digital` vs `Digital`). 2ª linha tem `observacao='duplicado conflitante'`. | 1 par | Dedup mantendo registro com `observacao` null (canônico); flag `_dq_status=warning` na linha vencedora com `"duplicata conflitante detectada"` | Mantém o primeiro registro como canônico |
| 24 | `id_canal` em lowercase: `ch07` | 1 registro | `UPPER(TRIM)` -> `CH07` | Padroniza |
| 25 | `nome_canal` null em CH06 (observacao explícita: "nome ausente") | 1 registro (12.5%) | **`_dq_status=rejected`** - sem nome, dim_canal não usável | Rejeita |
| 26 | `tipo_canal` caps mista: `Direto`/`Indireto`/`INDIRETO`/`Digital`/`digital` (5 variantes para 3 categorias lógicas) | 100% | `UPPER(TRIM)` -> `DIRETO`/`INDIRETO`/`DIGITAL` | Padroniza |
| 27 | `ativo` com 4 variantes: `sim`/`Sim`/`SIM`/`nao` | 100% | Mapeia para boolean (`SIM/S/TRUE/1`->true, `NAO/NÃO/N/FALSE/0`->false, resto->null) | Tipa corretamente |
| 28 | `ativo` null em CH07 | 1 registro (12.5%) | Mantém null + warning DQ | Mantém com warning |
| 29 | `observacao` 50% null | 4 registros | Esperado, campo opcional. Não trata. | Mantém |

### vendedores.csv

| # | Issue | Volume | Tratamento | Decisão |
|---|-------|--------|------------|---------|
| 30 | **Duplicatas reais**: V004 e V008 aparecem 2x | ~5% | Dedup por `seller_id` mantendo o registro com `hire_date` mais recente OU com `status='ativo'` | **Decisão crítica documentada** |
| 31 | V004 com `canal_id=CH99` (canal inválido?) | 1 registro | Validar contra `dim_canal` no Gold; se órfão, flag `warning` | Mantém com warning |
| 32 | V008 com "duplicado" no campo nome | 1 registro | Indica intenção do registro ser descartado; aplicar regra do issue 30 | Mantém |
| 33 | `canal_id` vazio | ~5% | `coalesce(canal_id, 'NAO_INFORMADO')` + flag | Mantém |
| 34 | `regional_code` com valores curtos: `S`, `SE`, `N`, `CO`, mas tb `sul`, `ch07` | ~10% | Lookup contra `silver.regioes` normalizado | Padroniza |
| 35 | `status` em caps mista: `Ativo`, `ativo`, `inativo`, vazio | ~30% | `UPPER` + default `INATIVO` se vazio | Mantém |
| 36 | `hire_date` em 3 formatos | ~50% | Multi-format parse (issue 1) | Mantém |

### legado_regioes_pipe.txt

| # | Issue | Volume | Tratamento | Decisão |
|---|-------|--------|------------|---------|
| 37 | **Duplicatas lógicas**: `S`/`Sul` vs `sul`/`Sul`, `SE`/`Sudeste` vs `SE`/`sao paulo` (2 registros para SE) | ~30% | Lookup table canônica + dedup por `regional_code` UPPER | Padroniza |
| 38 | `regional_code='XX'` artificial (placeholder pra "sem região") | 1 registro | Mapear para `N/A` ou manter como flag `regional_code IS NULL` | Decisão: mantém como `XX` com label "Não informada" |
| 39 | `manager_name` lower case ("sao paulo" sem acento) | ~20% | `INITCAP` + correção manual do "sao paulo" -> "São Paulo" | Mantém |
| 40 | `active_flag` como int (0/1) | 100% | `cast(boolean)` | Padroniza |

### atendimento_ocorrencias.ndjson

| # | Issue | Volume | Tratamento | Decisão |
|---|-------|--------|------------|---------|
| 41 | `event_type` null | ~5% | Flag DQ `warning`; default `NAO_CLASSIFICADO` | Mantém com warning |
| 42 | `severity` null | ~10% | Flag DQ; default `MEDIUM` | Mantém com warning |
| 43 | `status` null OU caps mista (`open`, `Open`, `closed`) | ~15% | `UPPER` + default `OPEN` se null | Mantém |
| 44 | `created_at` em 3 formatos | ~30% | Multi-format parse | Mantém |
| 45 | Gap semântico entre `refund` (fiscal) e `troca` (operacional) | - | Documentar, não tratar - é decisão de negócio | Mantém ambos |

### logistica_entregas.json

| # | Issue | Volume | Tratamento | Decisão |
|---|-------|--------|------------|---------|
| 46 | Estrutura aninhada (`carrier{}`, `timestamps{}`, `destination{}`) | 100% | Flatten via dot notation no select | Mantém |
| 47 | `carrier.name` esparso (null) | ~15% | Flag DQ + default `TRANSPORTADORA_NAO_INFORMADA` | Mantém com warning |
| 48 | `delivery_status` em PT (`atrasado`, `delivered`) | 100% | Padronização para inglês ou PT, escolher um | Decisão: padroniza para PT (`ATRASADO`, `ENTREGUE`, `EM_TRANSITO`) |
| 49 | `delivery_status` null | ? | Flag `rejected` (não dá pra inferir) | Mantém com `rejected` |
| 50 | Timestamps multi-formato | 100% | Multi-format parse | Mantém |
| 51 | Cálculo de atraso: `delivered_at` > `promised_date` requer join com pedido | 100% | Join no Silver de entregas com pedidos cabec -> calcular `atraso_dias` | Implementa |

---

## Resumo quantitativo esperado (a validar pós-Silver)

| Tabela Silver | Linhas Bronze | Linhas Silver | DQ clean | DQ warning | DQ rejected |
|---|---|---|---|---|---|
| pedidos_cabecalho | 403 | 403 (sem dedup esperada) | ~80% | ~20% | 0 |
| pedidos_itens | 995 | 995 | ~80% | ~20% (item_status null) | 0 |
| produtos | 65 | 65 | ~95% | ~5% | 0 |
| clientes | 183 | 180 (após dedup customer_id) | ~50% (limitado por nulls em segmento/porte) | ~45% (warnings) | ~5% (email inválido) |
| canais | 8 | 7 (após dedup CH05 conflitante) | ~57% (4 canais limpos) | ~29% (2 com warning: dup ou ativo null) | ~14% (CH06 sem nome) |
| vendedores | 42 | 40 (após dedup V004/V008) | ~70% | ~30% | 0 |
| regioes | 9 | 7-8 (após dedup S/sul, SE) | ~70% | ~30% | 0 |
| ocorrencias | 269 | 269 | ~70% | ~25% | ~5% |
| entregas | ~1700 | ~1700 | ~80% | ~15% | ~5% |

**Validação**: o notebook `99_validation` confirma que os números reais batem com essas estimativas (com tolerância) e marca as divergências.

---

## Premissas adotadas

1. **Status de pedido canônico**: `FATURADO`, `EM_SEPARACAO`, `CANCELADO` (qualquer outro vira `OUTRO`)
2. **Moeda padrão**: BRL (todos os valores monetários assumidos em Real)
3. **Timezone**: assumido America/Sao_Paulo para timestamps sem TZ explícita
4. **Vendedor V004/V008 duplicado**: vence o registro com `hire_date` mais recente; em caso de empate, vence `status='ativo'`
5. **Região "XX"**: tratada como categoria válida com label "Não informada", não como erro
6. **Carrier null**: assume `TRANSPORTADORA_NAO_INFORMADA`, mas registra warning para business follow-up
7. **Data sem ano explícito**: assume ano corrente (2025/2026 conforme contexto do dataset)

---

## Limitações reconhecidas

- **Sem definição de SLA por tipo de ocorrência**: o `severity_score` é heurístico (HIGH=3, MEDIUM=2, LOW=1).
- **Sem validação cross-source histórica**: não há base anterior para comparar volumes; alertas só capturam outliers do snapshot atual.
- **`UF_MAP` em `02_silver_clientes` é exaustivo para os 18 valores observados**: se a planilha ganhar novos estados/typos, o registro cai como warning (`uf is null`). Em produção, seria substituído por uma tabela de referência versionada em Delta.
