# ADR-002: SCD Type 1 padrão + SCD Type 2 demonstrativa em dim_cliente_history

## Status

Aceito — 2026-05-11.

## Contexto

O modelo dimensional do case tem 6 dimensões: `dim_cliente`, `dim_produto`, `dim_canal`, `dim_vendedor`, `dim_regiao`, `dim_data`. Para cada uma, é necessário decidir o tipo de Slowly Changing Dimension (SCD) — ou seja, como tratar mudanças nos atributos das entidades ao longo do tempo.

As opções relevantes:

- **SCD Type 1** (overwrite): a dimensão sempre reflete o estado atual. Mudanças sobrescrevem valores antigos. Análise histórica fica impossível.
- **SCD Type 2** (versionamento): cada mudança gera uma nova linha com `effective_date` / `end_date` / `is_current`. Análise histórica preservada, mas complexidade aumenta (range joins, hash de tracking, MERGE pattern).
- **SCD Type 3** (one previous value): coluna adicional com valor anterior. Útil para dimensões com pouca volatilidade.
- **SCD Type 4** (mini-dimension separada): variantes Kimball para alta cardinalidade.

O dataset do case é one-shot (XLSX exportado, sem business date de atualização). Não há histórico real de mudanças disponível. Em produção, mudanças relevantes seriam: cliente muda de segmento (`PEQUENO`→`MEDIO`), cliente muda de cidade, vendedor sai do quadro (`status: ATIVO`→`INATIVO`), produto muda de categoria.

O budget de 25h não permite implementar SCD2 em todas as dimensões (estimativa: 4h por dimensão × 5 = 20h). Mas implementar zero SCD2 não demonstra domínio do tema — gap em avaliação senior.

## Decisão

Adotar a seguinte configuração:

- **SCD Type 1** padrão para todas as 6 dimensões (overwrite via `mode("overwrite") + overwriteSchema=true`). Documentado explicitamente na coluna "Tipo SCD" de cada subseção em `data_model.md`.
- **SCD Type 2 demonstrativa em tabela paralela `dim_cliente_history`** — não substitui `dim_cliente` SCD1; coexistem.

### Design da SCD2 em dim_cliente_history

Colunas tracking (4 atributos cuja mudança gera nova versão): `cliente_segmento`, `estado`, `cidade`, `status`.

Hash MD5 sobre essas 4 colunas (com `coalesce(col, '')` para tratar nulos) determina se houve mudança:

```python
F.md5(F.concat_ws("||",
    F.coalesce(F.col("cliente_segmento"), F.lit("")),
    F.coalesce(F.col("estado"), F.lit("")),
    F.coalesce(F.col("cidade"), F.lit("")),
    F.coalesce(F.col("status"), F.lit("")),
)).alias("scd_hash")
```

Colunas de versionamento:
- `effective_date` = `_ingestion_timestamp` da Silver (documentado abaixo como surrogate).
- `end_date` = `'9999-12-31'` (sentinel) na versão atual; `current_date()` quando uma nova versão fecha a anterior.
- `is_current` BOOLEAN — redundante com `end_date='9999-12-31'`, mantido por idiomática em ferramentas BI (Tableau, Power BI filtram boolean nativamente, acelerando filtros do consumidor).

Pattern de carga:
- **Primeira ingestão:** `CREATE TABLE IF NOT EXISTS` + write completo, todos com `is_current=true`.
- **Ingestões subsequentes:** Delta `MERGE INTO`. Detecta mudanças via diff de hash. Fecha versão antiga (`is_current=false`, `end_date=current_date()`); insere nova versão (`is_current=true`).

Chave: natural (`customer_code`), sem surrogate key. Joins de fatos com versão histórica usam range join:

```sql
SELECT f.*, h.cliente_segmento
FROM fact_pedido f
JOIN dim_cliente_history h
  ON f.customer_code = h.customer_code
  AND CAST(f.data_id AS DATE) BETWEEN h.effective_date AND h.end_date
```

### `effective_date` como surrogate para business date

O dataset não tem campo "data da última atualização" no CRM. Documentamos explicitamente: `effective_date` representa **data de detecção da mudança pelo pipeline** (proxy via `_ingestion_timestamp`), **não data de ocorrência da mudança no sistema de origem**. Em produção, o ideal seria capturar `crm_updated_at` da fonte e usar como `effective_date`.

## Alternativas rejeitadas

1. **Substituir `dim_cliente` por SCD2**: rejeitado — quebraria `vw_kpi_business` que assume `customer_code` único na dim. Exigiria refatorar toda a view e queries do BI Runbook.

2. **SCD Type 2 em todas as 5 dimensões dinâmicas (excluir dim_data)**: rejeitado por orçamento — 4h × 5 = 20h, esgotando o budget para apresentação e governança.

3. **Surrogate key (`dim_cliente_sk`)**: rejeitado para o escopo desta decisão. Surrogate key eliminaria o range join e aceleraria queries em produção. Documentado como follow-up: "em ingestões reais com volume e queries frequentes, adotar surrogate key seria recomendado". Para o case demonstrativo, range join em dataset de 180 clientes é performance-irrelevante.

4. **Hash sem `status`**: rejeitado — perderia mudança crítica de negócio (cliente ativo → inativo). Hash deve incluir todas as colunas cuja mudança altera análise.

5. **SCD Type 3** (one previous value): rejeitado — não cobre histórico arbitrário (>2 versões); inadequado para "ver perfil do cliente em qualquer data".

## Consequências

**Positivas:**

- Demonstração efetiva de competência em SCD2 — pattern completo (hash, MERGE, range join, sentinel) implementado e documentado.
- `dim_cliente` original preservada — zero impacto em consumidores existentes (`vw_kpi_business` continua funcionando).
- Range join em PT-BR pode ser ensinado ao analista BI no `BI_RUNBOOK.md`.
- ADR-008 (NOVO) documentará o vocabulário de tags incluindo `scd_type` como tag potencial em iteração futura.

**Negativas:**

- Tabela paralela duplica armazenamento e complica modelo (analistas precisam decidir qual usar). Mitigado pela documentação clara em `data_model.md` e `BI_RUNBOOK.md`: "use `dim_cliente` para análise atual; use `dim_cliente_history` para análise histórica de perfil".
- Range join é mais caro que equi-join na execução; aceito por baixo volume.
- `effective_date` como proxy para business date é honest mas tecnicamente imperfeito; documentado como limitação consciente.
- Demais 4 dimensões (produto, canal, vendedor, regiao) ficam sem histórico. Aceito como YAGNI no escopo do case.

**Trade-off aceito:** demonstrar uma vez SCD2 com profundidade documentada > implementar SCD2 raso em todas as dimensões > implementar zero SCD2.

## Referências

- ADR-001 (Bronze string-typed — base de dados para o SCD2)
- ADR-005 (DQ flags + quarantine — silver clean alimenta dim_cliente_history)
- `notebooks/03_gold/03_gold_dimensions.py` — implementação da SCD2
- `docs/data_model.md` — bus matrix + SCD type por dim
- Kimball Group, "Slowly Changing Dimension Techniques" (referência canônica do pattern)
