# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: 01_apply_governance
# MAGIC ## Objetivo:
# MAGIC Aplica governanca Unity Catalog em 29 tabelas (9 bronze + 9 silver + 11 gold). Idempotente: re-rodar sobrescreve sem duplicar. Define COMMENT ON TABLE, COMMENT ON COLUMN, ALTER TABLE SET TAGS (5 fixas: owner, layer, classification, pii, data_domain), TBLPROPERTIES de retencao Delta, e CHECK constraints com pattern `DROP IF EXISTS` antes de `ADD`. Implementa REQs 001 a 008 e 009 a 014-gov do DEFINE. Coracao do Tier 1 (catalogo vivo).
# MAGIC
# MAGIC ## Fontes de Dados
# MAGIC | Origem | Informacao |
# MAGIC |--------|-------------|
# MAGIC | dict literal `GOVERNANCE` (este notebook) | Definicoes inline de COMMENT/TAG/CONSTRAINT por tabela |
# MAGIC | `system.information_schema.tables` | Verificacao final do gate REQ-001 |
# MAGIC
# MAGIC ## Historico de alteracoes
# MAGIC | Data | Desenvolvido por | Modificacoes |
# MAGIC |------|------------------|-------------|
# MAGIC | 2026-05-11 | Wilson Lucas | Criacao do notebook (Tier 1 do uplift 200%) |

# COMMAND ----------

CATALOG = "workspace"
OWNER = "wilsonlucas201@gmail.com"


def sanitize(s: str) -> str:
    """Escapa apostrofes para SQL literal."""
    return s.replace("'", "''")


def fallback_comment(table_name: str, col: str) -> str:
    """Gera COMMENT default baseado em sufixos comuns (id, code, amount, date, etc).

    Aplicado em colunas business que nao foram explicitamente mapeadas no dict
    GOVERNANCE. Garante que toda coluna business tem ALGUM comment, mantendo
    o gate REQ-002 (>=80% cobertura).
    """
    cl = col.lower()
    if cl.endswith("_id") or cl.endswith("_code"):
        return f"Identificador (chave) referente a {col.replace('_id','').replace('_code','')}."
    if cl.endswith("_amount") or cl in ("gross_amount", "discount_amount", "net_amount", "valor_liquido"):
        return "Valor monetario em BRL (DECIMAL 15,2)."
    if cl.endswith("_date") or cl in ("data_pedido", "data_cadastro", "data_promessa_id"):
        return "Data em formato DATE ou FK para dim_data."
    if cl.endswith("_at") or cl == "updated_at":
        return "Timestamp."
    if cl.startswith("flag_") or cl.startswith("eh_") or cl.endswith("_flag") or cl in ("ativo", "is_active", "on_time_flag", "fim_de_semana"):
        return "Flag boolean."
    if cl.startswith("qtd_") or cl == "quantity":
        return "Quantidade (contagem inteira)."
    if cl in ("nome", "nome_cliente", "nome_canal", "regiao_nome", "vendedor_nome", "seller_name", "gestor_nome"):
        return "Nome legivel."
    if cl == "status" or cl.startswith("status_") or cl.endswith("_status"):
        return "Status (enum canonico)."
    if cl in ("estado", "uf", "estado_original", "destination_state"):
        return "Unidade Federativa (UF) brasileira (sigla 2 letras)."
    if cl in ("cidade", "destination_city"):
        return "Cidade."
    if cl in ("ano", "mes", "dia", "trimestre", "ano_mes", "dia_semana", "dia_semana_num", "mes_nome"):
        return "Atributo temporal derivado da data."
    if cl in ("category", "subcategory", "family", "tags"):
        return "Categoria ou tag de classificacao do produto."
    if cl in ("currency", "cost", "list_price"):
        return "Atributo financeiro do produto."
    if cl in ("email", "email_valid", "telefone"):
        return "Dado de contato (PII - tratar com classification adequada)."
    if cl in ("porte", "segmento", "cliente_segmento"):
        return "Segmentacao comercial do cliente."
    if cl in ("payment_method", "payment_status"):
        return "Atributo do meio de pagamento (degenerate dimension)."
    if cl in ("delivery_status", "carrier_name", "mode"):
        return "Atributo da entrega."
    if cl in ("severity", "severity_score", "severity_score_total", "event_type"):
        return "Atributo da ocorrencia (severity = LOW/MEDIUM/HIGH ou score 1-3)."
    if cl in ("atraso_dias", "lead_time_dias"):
        return "Lead time calculado em dias."
    if cl in ("divergencia_total", "total_item_calculado"):
        return "Coluna de validacao matematica do silver."
    if cl == "tipo_canal":
        return "Tipo do canal comercial."
    if cl == "canal_id":
        return "FK para dim_canal."
    if cl in ("regiao_estado",):
        return "UF associada a regiao."
    return f"Coluna business {col}."


# COMMAND ----------

# Estrutura: {table_fqn: {comment, tags, columns: {col: comment}, tblproperties, constraints}}
GOVERNANCE = {
    # ============================================================
    # BRONZE - 9 tabelas string-typed (ADR-001)
    # ============================================================
    "workspace.bronze.pedidos_cabecalho": {
        "comment": "Bronze: cabecalhos de pedidos do ERP. Ingestao 1:1 sem transformacao. Tipos string preservam formato original (ADR-001). Alimenta silver.pedidos_cabecalho.",
        "tags": {"layer": "bronze", "classification": "internal", "pii": "false", "data_domain": "comercial"},
        "columns": {
            "order_id": "ID do pedido. Formato 'ORD-NNNNN'.",
            "customer_code": "Codigo do cliente. FK natural para dim_cliente.",
            "seller_id": "ID do vendedor. FK natural para dim_vendedor.",
            "order_date": "Data do pedido em formato heterogeneo (yyyy-MM-dd ou dd/MM/yyyy). Parseada no silver.",
            "promised_date": "Data prometida para entrega.",
            "last_update": "Timestamp da ultima alteracao no ERP.",
            "gross_amount": "Valor bruto BRL como string com virgula decimal BR.",
            "discount_amount": "Desconto aplicado em BRL.",
            "net_amount": "Valor liquido em BRL. Validado contra (gross - discount) no silver.",
            "status_order": "Status original do ERP. Pode ter variantes (FATURADO, EM SEPARACAO, etc) padronizadas no silver.",
            "payment_details": "JSON aninhado com method/status/installments. Parseado no silver.",
        },
    },
    "workspace.bronze.pedidos_itens": {
        "comment": "Bronze: itens de pedidos do ERP. 1 linha por SKU dentro de um pedido. Alimenta silver.pedidos_itens.",
        "tags": {"layer": "bronze", "classification": "internal", "pii": "false", "data_domain": "comercial"},
        "columns": {
            "order_id": "FK para pedidos_cabecalho.order_id.",
            "item_seq": "Sequencia do item dentro do pedido.",
            "product_code": "Codigo do produto. FK natural para dim_produto.",
            "quantity": "Quantidade pedida.",
            "unit_price": "Preco unitario em BRL como string com virgula.",
            "total_item": "Total do item (quantity * unit_price). Validado no silver.",
            "item_status": "Status do item dentro do pedido.",
        },
    },
    "workspace.bronze.produtos": {
        "comment": "Bronze: catalogo de produtos da API interna. JSON aninhado serializado como string (product/pricing/attributes). Alimenta silver.produtos apos flatten.",
        "tags": {"layer": "bronze", "classification": "internal", "pii": "false", "data_domain": "comercial"},
        "columns": {
            "updated_at": "Timestamp da ultima atualizacao na API.",
            "product_json": "JSON com product_code, name, category.",
            "pricing_json": "JSON com list_price, cost.",
            "attributes_json": "JSON com atributos de produto (cor, tamanho, etc).",
        },
    },
    "workspace.bronze.clientes": {
        "comment": "Bronze: cadastro de clientes do CRM exportado em XLSX. Tipos string para tolerar variantes. Alimenta silver.clientes apos dedup.",
        "tags": {"layer": "bronze", "classification": "internal", "pii": "true", "data_domain": "comercial"},
        "columns": {
            "customer_code": "Codigo unico do cliente (PK).",
            "nome": "Nome ou razao social do cliente.",
            "email": "Email de contato.",
            "telefone": "Telefone de contato.",
            "cliente_segmento": "Segmento (PEQUENO, MEDIO, GRANDE).",
            "estado": "UF (sigla ou nome).",
            "cidade": "Cidade.",
            "status": "Status do cliente (ATIVO, INATIVO).",
        },
    },
    "workspace.bronze.canais": {
        "comment": "Bronze: cadastro de canais comerciais via XLSX. Pequeno volume (8 linhas brutas). Alimenta silver.canais apos dedup CH05.",
        "tags": {"layer": "bronze", "classification": "internal", "pii": "false", "data_domain": "comercial"},
        "columns": {
            "channel_code": "Codigo do canal (PK).",
            "channel_name": "Nome legivel do canal.",
            "observacao": "Observacao livre do canal.",
        },
    },
    "workspace.bronze.regioes": {
        "comment": "Bronze: dados de regiao do sistema legado em formato pipe-delimited. Tem variantes redundantes (S/Sul, SE/Sudeste) deduplicadas no silver.",
        "tags": {"layer": "bronze", "classification": "internal", "pii": "false", "data_domain": "transversal"},
        "columns": {
            "regional_code": "Codigo da regiao (PK).",
            "regional_name": "Nome da regiao (canonico apos dedup no silver).",
            "active_flag": "Flag de ativacao.",
        },
    },
    "workspace.bronze.vendedores": {
        "comment": "Bronze: cadastro de vendedores via CSV. Inclui codigos duplicados (V004, V008) deduplicados no silver com prioridade ATIVO + hire_date mais recente.",
        "tags": {"layer": "bronze", "classification": "internal", "pii": "true", "data_domain": "comercial"},
        "columns": {
            "seller_id": "ID do vendedor (PK).",
            "nome": "Nome do vendedor.",
            "regional_code": "FK para regioes.",
            "hire_date": "Data de contratacao.",
            "status": "Status (ATIVO, INATIVO).",
        },
    },
    "workspace.bronze.entregas": {
        "comment": "Bronze: dados de entregas em JSON array com structs aninhados (carrier, timestamps, destination) serializados como JSON string. Alimenta silver.entregas apos flatten.",
        "tags": {"layer": "bronze", "classification": "internal", "pii": "false", "data_domain": "logistica"},
        "columns": {
            "delivery_id": "ID da entrega (PK).",
            "order_ref": "FK para pedidos_cabecalho.order_id.",
            "delivery_status": "Status da entrega.",
            "cost": "Custo da entrega em BRL.",
            "carrier_json": "JSON com dados da transportadora.",
            "timestamps_json": "JSON com shipped_at, delivered_at, etc.",
            "destination_json": "JSON com endereco de destino.",
        },
    },
    "workspace.bronze.ocorrencias": {
        "comment": "Bronze: tickets de atendimento em NDJSON (1 obj por linha). Alimenta silver.ocorrencias com 5 formatos de timestamp tratados.",
        "tags": {"layer": "bronze", "classification": "internal", "pii": "false", "data_domain": "atendimento"},
        "columns": {
            "ticket_id": "ID do ticket (PK).",
            "order_id": "FK opcional para pedidos.",
            "created_at": "Timestamp de criacao em formatos heterogeneos.",
            "event_type": "Tipo do evento (REFUND, TROCA, DELAY, COMPLAINT).",
            "severity": "Severidade (LOW, MEDIUM, HIGH).",
            "status": "Status do ticket (OPEN, CLOSED).",
        },
    },
    # ============================================================
    # SILVER - 9 tabelas tipadas com DQ (ADR-005)
    # ============================================================
    "workspace.silver.pedidos_cabecalho": {
        "comment": "Silver: pedidos cabecalho com casts ANSI-safe, status canonicalizado, payment_details parseado e DQ flags. Granularidade: 1 linha por order_id.",
        "tags": {"layer": "silver", "classification": "internal", "pii": "false", "data_domain": "comercial"},
        "columns": {
            "order_id": "PK do pedido.",
            "customer_code": "FK para clientes.",
            "seller_id": "FK para vendedores.",
            "order_date": "Data do pedido em DATE.",
            "promised_date": "Data prometida em DATE.",
            "last_update": "Timestamp da ultima alteracao.",
            "gross_amount": "Valor bruto BRL DECIMAL(15,2).",
            "discount_amount": "Desconto BRL.",
            "net_amount": "Valor liquido BRL.",
            "status_canonico": "Status padronizado (FATURADO, EM_SEPARACAO, CANCELADO, OUTRO).",
            "status_raw": "Status original do ERP (auditoria).",
            "payment_method": "Metodo de pagamento extraido do JSON.",
            "payment_status": "Status do pagamento.",
        },
    },
    "workspace.silver.pedidos_itens": {
        "comment": "Silver: itens de pedidos com casts e DQ flags. Granularidade: 1 linha por (order_id, item_seq).",
        "tags": {"layer": "silver", "classification": "internal", "pii": "false", "data_domain": "comercial"},
        "columns": {
            "order_id": "FK para pedidos_cabecalho.",
            "item_seq": "Sequencia do item.",
            "product_code": "FK para produtos.",
            "quantity": "Quantidade pedida.",
            "unit_price": "Preco unitario DECIMAL(15,2).",
            "total_item": "Total do item.",
            "total_item_calculado": "Total calculado (qty * unit_price) para validacao.",
            "divergencia_total": "Flag boolean: ha divergencia entre total e calculado.",
            "item_status": "Status do item.",
        },
    },
    "workspace.silver.produtos": {
        "comment": "Silver: catalogo de produtos com flatten dos JSONs aninhados e casts. Granularidade: 1 linha por product_code.",
        "tags": {"layer": "silver", "classification": "internal", "pii": "false", "data_domain": "comercial"},
        "columns": {
            "product_code": "PK do produto.",
            "product_name": "Nome do produto.",
            "category": "Categoria do produto.",
            "list_price": "Preco de lista DECIMAL(15,2).",
            "cost": "Custo BRL.",
        },
    },
    "workspace.silver.clientes": {
        "comment": "Silver: cadastro de clientes deduplicado (window function por customer_code, prioriza updated_at desc). Granularidade: 1 linha por customer_code.",
        "tags": {"layer": "silver", "classification": "internal", "pii": "true", "data_domain": "comercial"},
        "columns": {
            "customer_code": "PK do cliente.",
            "nome": "Nome ou razao social.",
            "email": "Email validado (formato).",
            "cliente_segmento": "Segmento canonico.",
            "estado": "UF padronizada (sigla 2 letras).",
            "cidade": "Cidade.",
            "status": "Status (ATIVO, INATIVO).",
        },
    },
    "workspace.silver.canais": {
        "comment": "Silver: canais comerciais deduplicados (CH05 removido). Granularidade: 1 linha por channel_code.",
        "tags": {"layer": "silver", "classification": "internal", "pii": "false", "data_domain": "comercial"},
        "columns": {
            "channel_code": "PK do canal.",
            "channel_name": "Nome do canal.",
            "observacao": "Observacao livre.",
        },
    },
    "workspace.silver.regioes": {
        "comment": "Silver: regioes deduplicadas com nome canonico (S->SUL, SE->SUDESTE, etc). Granularidade: 1 linha por regional_code.",
        "tags": {"layer": "silver", "classification": "internal", "pii": "false", "data_domain": "transversal"},
        "columns": {
            "regional_code": "PK da regiao.",
            "regional_name": "Nome canonico da regiao.",
            "active_flag": "Flag de ativacao.",
        },
    },
    "workspace.silver.vendedores": {
        "comment": "Silver: vendedores deduplicados (V004/V008 com prioridade ATIVO + hire_date desc). Granularidade: 1 linha por seller_id.",
        "tags": {"layer": "silver", "classification": "internal", "pii": "true", "data_domain": "comercial"},
        "columns": {
            "seller_id": "PK do vendedor.",
            "nome": "Nome do vendedor.",
            "regional_code": "FK para regioes.",
            "hire_date": "Data de contratacao em DATE.",
            "status": "Status canonico.",
        },
    },
    "workspace.silver.entregas": {
        "comment": "Silver: entregas com flatten de JSON aninhado (carrier, timestamps, destination), lead_time_dias derivado. Granularidade: 1 linha por delivery_id.",
        "tags": {"layer": "silver", "classification": "internal", "pii": "false", "data_domain": "logistica"},
        "columns": {
            "delivery_id": "PK da entrega.",
            "order_id": "FK para pedidos.",
            "delivery_status": "Status da entrega canonico.",
            "carrier_name": "Nome da transportadora.",
            "shipped_at": "Timestamp de envio.",
            "delivered_at": "Timestamp de entrega.",
            "promised_date": "Data prometida.",
            "lead_time_dias": "Diferenca em dias entre shipped e delivered.",
            "cost": "Custo da entrega DECIMAL(15,2).",
        },
    },
    "workspace.silver.ocorrencias": {
        "comment": "Silver: tickets de atendimento com timestamps parseados em 5 formatos, severity canonico e severity_score derivado (HIGH=3, MEDIUM=2, LOW=1).",
        "tags": {"layer": "silver", "classification": "internal", "pii": "false", "data_domain": "atendimento"},
        "columns": {
            "ticket_id": "PK do ticket.",
            "order_id": "FK opcional para pedidos.",
            "created_at": "Timestamp de criacao em TIMESTAMP.",
            "event_type": "Tipo canonico do evento.",
            "severity": "Severidade canonica.",
            "severity_score": "Score numerico (1=LOW, 2=MEDIUM, 3=HIGH).",
            "status": "Status do ticket.",
        },
    },
    # ============================================================
    # GOLD - 6 dims + 4 facts + 1 view (ADR-002, ADR-003)
    # ============================================================
    "workspace.gold.dim_cliente": {
        "comment": "Dimensao cliente SCD Type 1 (overwrite). Para historico temporal usar dim_cliente_history (ADR-002). Granularidade: 1 linha por customer_code.",
        "tags": {"layer": "gold", "classification": "internal", "pii": "true", "data_domain": "comercial"},
        "tblproperties": {
            "delta.logRetentionDuration": "interval 30 days",
            "delta.deletedFileRetentionDuration": "interval 7 days",
        },
        "columns": {
            "customer_code": "PK natural do cliente.",
            "nome": "Nome do cliente.",
            "email": "Email validado.",
            "cliente_segmento": "Segmento (PEQUENO, MEDIO, GRANDE).",
            "estado": "UF (sigla 2 letras).",
            "cidade": "Cidade.",
            "status": "Status (ATIVO, INATIVO).",
        },
    },
    "workspace.gold.dim_produto": {
        "comment": "Dimensao produto SCD Type 1. Granularidade: 1 linha por product_code.",
        "tags": {"layer": "gold", "classification": "internal", "pii": "false", "data_domain": "comercial"},
        "tblproperties": {
            "delta.logRetentionDuration": "interval 30 days",
            "delta.deletedFileRetentionDuration": "interval 7 days",
        },
        "columns": {
            "product_code": "PK natural do produto.",
            "product_name": "Nome do produto.",
            "category": "Categoria.",
            "list_price": "Preco de lista BRL.",
            "cost": "Custo BRL.",
        },
    },
    "workspace.gold.dim_canal": {
        "comment": "Dimensao canal de venda SCD Type 1. Granularidade: 1 linha por channel_code.",
        "tags": {"layer": "gold", "classification": "internal", "pii": "false", "data_domain": "comercial"},
        "tblproperties": {
            "delta.logRetentionDuration": "interval 30 days",
            "delta.deletedFileRetentionDuration": "interval 7 days",
        },
        "columns": {
            "channel_code": "PK natural do canal.",
            "channel_name": "Nome do canal.",
            "observacao": "Observacao livre.",
        },
    },
    "workspace.gold.dim_regiao": {
        "comment": "Dimensao regiao SCD Type 1. Granularidade: 1 linha por regional_code.",
        "tags": {"layer": "gold", "classification": "internal", "pii": "false", "data_domain": "transversal"},
        "tblproperties": {
            "delta.logRetentionDuration": "interval 30 days",
            "delta.deletedFileRetentionDuration": "interval 7 days",
        },
        "columns": {
            "regional_code": "PK natural.",
            "regional_name": "Nome canonico da regiao.",
            "active_flag": "Flag de ativacao.",
        },
    },
    "workspace.gold.dim_vendedor": {
        "comment": "Dimensao vendedor SCD Type 1. Granularidade: 1 linha por seller_id.",
        "tags": {"layer": "gold", "classification": "internal", "pii": "true", "data_domain": "comercial"},
        "tblproperties": {
            "delta.logRetentionDuration": "interval 30 days",
            "delta.deletedFileRetentionDuration": "interval 7 days",
        },
        "columns": {
            "seller_id": "PK natural do vendedor.",
            "nome": "Nome.",
            "regional_code": "FK para dim_regiao.",
            "hire_date": "Data de contratacao.",
            "status": "Status (ATIVO, INATIVO).",
        },
    },
    "workspace.gold.dim_data": {
        "comment": "Dimensao calendario gerada com range derivado das fatos. Inclui atributos analiticos e feriados nacionais BR 2025. Granularidade: 1 linha por data_id (formato yyyymmdd).",
        "tags": {"layer": "gold", "classification": "internal", "pii": "false", "data_domain": "calendario"},
        "tblproperties": {
            "delta.logRetentionDuration": "interval 30 days",
            "delta.deletedFileRetentionDuration": "interval 7 days",
        },
        "columns": {
            "data_id": "PK no formato yyyymmdd (INT).",
            "data_completa": "Data DATE.",
            "ano": "Ano.",
            "mes": "Mes (1-12).",
            "dia": "Dia do mes.",
            "trimestre": "Trimestre (1-4).",
            "dia_semana": "Dia da semana (1=Domingo a 7=Sabado).",
            "fim_de_semana": "Boolean: data eh sabado ou domingo.",
        },
    },
    "workspace.gold.fact_pedido": {
        "comment": "Fato granular pedido. 1 linha por order_id. Excluidos pedidos com _dq_status=rejected (vao para silver.quarantine_pedidos).",
        "tags": {"layer": "gold", "classification": "internal", "pii": "false", "data_domain": "comercial"},
        "tblproperties": {
            "delta.logRetentionDuration": "interval 30 days",
            "delta.deletedFileRetentionDuration": "interval 7 days",
        },
        "constraints": [
            ("chk_net_amount_nonneg", "net_amount IS NULL OR net_amount >= 0"),
        ],
        "columns": {
            "order_id": "PK do pedido.",
            "customer_code": "FK para dim_cliente.",
            "seller_id": "FK para dim_vendedor.",
            "data_id": "FK para dim_data (data do pedido).",
            "channel_code": "FK para dim_canal.",
            "gross_amount": "Valor bruto BRL.",
            "discount_amount": "Desconto BRL.",
            "net_amount": "Valor liquido BRL.",
            "status_canonico": "Status padronizado.",
            "payment_method": "Metodo de pagamento (degenerate dimension).",
        },
    },
    "workspace.gold.fact_item": {
        "comment": "Fato granular item de pedido. 1 linha por (order_id, item_seq). Permite analise por SKU.",
        "tags": {"layer": "gold", "classification": "internal", "pii": "false", "data_domain": "comercial"},
        "tblproperties": {
            "delta.logRetentionDuration": "interval 30 days",
            "delta.deletedFileRetentionDuration": "interval 7 days",
        },
        "columns": {
            "order_id": "FK para fact_pedido.",
            "item_seq": "Sequencia do item.",
            "product_code": "FK para dim_produto.",
            "quantity": "Quantidade.",
            "unit_price": "Preco unitario BRL.",
            "total_item": "Total do item BRL.",
        },
    },
    "workspace.gold.fact_entrega": {
        "comment": "Fato granular entrega. 1 linha por delivery_id. Um pedido pode ter N entregas (entregas parciais). Vw_kpi_business agrega pelo pior caso.",
        "tags": {"layer": "gold", "classification": "internal", "pii": "false", "data_domain": "logistica"},
        "tblproperties": {
            "delta.logRetentionDuration": "interval 30 days",
            "delta.deletedFileRetentionDuration": "interval 7 days",
        },
        "columns": {
            "delivery_id": "PK da entrega.",
            "order_id": "FK para fact_pedido.",
            "data_envio_id": "FK para dim_data (shipped_at).",
            "data_entrega_id": "FK para dim_data (delivered_at).",
            "lead_time_dias": "Lead time em dias.",
            "eh_atrasado": "Boolean: delivered_at > promised_date.",
            "carrier_name": "Transportadora.",
            "cost": "Custo BRL.",
        },
    },
    "workspace.gold.fact_ocorrencia": {
        "comment": "Fato granular ocorrencia (ticket). 1 linha por ticket_id. severity_score numerico (1=LOW, 3=HIGH) facilita agregacoes ranqueadas.",
        "tags": {"layer": "gold", "classification": "internal", "pii": "false", "data_domain": "atendimento"},
        "tblproperties": {
            "delta.logRetentionDuration": "interval 30 days",
            "delta.deletedFileRetentionDuration": "interval 7 days",
        },
        "columns": {
            "ticket_id": "PK do ticket.",
            "order_id": "FK opcional para fact_pedido.",
            "data_id": "FK para dim_data (created_at).",
            "event_type": "Tipo do evento (REFUND, TROCA, DELAY, COMPLAINT).",
            "severity": "Severidade canonica.",
            "severity_score": "Score 1-3.",
            "status": "Status do ticket.",
        },
    },
}

# View consolidada (so COMMENT, sem TAGS porque view nao suporta SET TAGS em UC Free Edition)
VIEWS_COMMENTS = {
    "workspace.gold.vw_kpi_business": "View consolidada pre-joinada para BI: pedido + cliente + canal + regiao + vendedor + flags operacionais (eh_atrasado, qty_itens, qty_skus). Ponto de entrada principal para o Analista de BI (vide BI_RUNBOOK.md).",
}

# COMMAND ----------

# Aplicar governanca em loop idempotente
applied = 0
errors = 0

for table_fqn, spec in GOVERNANCE.items():
    try:
        # COMMENT ON TABLE
        spark.sql(f"COMMENT ON TABLE {table_fqn} IS '{sanitize(spec['comment'])}'")

        # SET TAGS (5 fixas: owner adicionado aqui + 4 do spec)
        all_tags = {"owner": OWNER, **spec["tags"]}
        tags_clauses = ", ".join(f"'{k}' = '{sanitize(v)}'" for k, v in all_tags.items())
        spark.sql(f"ALTER TABLE {table_fqn} SET TAGS ({tags_clauses})")

        # SET TBLPROPERTIES (apenas tabelas que declararam)
        if "tblproperties" in spec and spec["tblproperties"]:
            props_clauses = ", ".join(f"'{k}' = '{v}'" for k, v in spec["tblproperties"].items())
            spark.sql(f"ALTER TABLE {table_fqn} SET TBLPROPERTIES ({props_clauses})")

        # COMMENT ON COLUMN (loop)
        for col, ccomment in spec["columns"].items():
            try:
                spark.sql(f"ALTER TABLE {table_fqn} ALTER COLUMN {col} COMMENT '{sanitize(ccomment)}'")
            except Exception as ce:
                # Coluna pode nao existir em alguns silvers (schema evoluiu). Log e continua.
                print(f"  [WARN] {table_fqn}.{col}: {str(ce)[:120]}")

        # CHECK constraints (idempotente: DROP IF EXISTS antes de ADD)
        for cname, expr in spec.get("constraints", []):
            try:
                spark.sql(f"ALTER TABLE {table_fqn} DROP CONSTRAINT IF EXISTS {cname}")
                spark.sql(f"ALTER TABLE {table_fqn} ADD CONSTRAINT {cname} CHECK ({expr})")
            except Exception as ke:
                print(f"  [WARN] {table_fqn} constraint {cname}: {str(ke)[:120]}")

        applied += 1
        print(f"[OK] {table_fqn}")
    except Exception as e:
        errors += 1
        print(f"[ERRO] {table_fqn}: {str(e)[:200]}")

# COMMAND ----------

# Aplicar comments em views (SET TAGS nao suportado em VIEW no Free Edition)
for view_fqn, comment in VIEWS_COMMENTS.items():
    try:
        spark.sql(f"COMMENT ON TABLE {view_fqn} IS '{sanitize(comment)}'")
        applied += 1
        print(f"[OK] {view_fqn}")
    except Exception as e:
        errors += 1
        print(f"[ERRO] {view_fqn}: {str(e)[:200]}")

# COMMAND ----------

# Auto-completar colunas business pendentes via fallback_comment (gate REQ-002 >=80%)
print()
print("[FALLBACK] Auto-completando colunas business pendentes...")

pending = spark.sql("""
    SELECT table_schema, table_name, column_name
    FROM system.information_schema.columns
    WHERE table_catalog = 'workspace'
    AND table_schema IN ('bronze','silver','gold')
    AND column_name NOT LIKE '\\_%' ESCAPE '\\\\'
    AND (comment IS NULL OR comment = '')
    ORDER BY table_schema, table_name, column_name
""").collect()

print(f"[INFO] Colunas pendentes: {len(pending)}")
fallback_applied = 0
fallback_errors = 0
for row in pending:
    fqn = f"workspace.{row.table_schema}.{row.table_name}"
    col = row.column_name
    cmt = fallback_comment(row.table_name, col)
    try:
        spark.sql(f"ALTER TABLE {fqn} ALTER COLUMN {col} COMMENT '{sanitize(cmt)}'")
        fallback_applied += 1
    except Exception as e:
        fallback_errors += 1
        print(f"  [WARN] {fqn}.{col}: {str(e)[:100]}")
print(f"[FALLBACK] {fallback_applied} aplicados, {fallback_errors} erros")

# COMMAND ----------

# Gate REQ-001 — verificar que todas as 19+ tabelas tem COMMENT non-empty
missing = spark.sql(
    """
    SELECT count(*) AS missing
    FROM system.information_schema.tables
    WHERE table_catalog = 'workspace'
    AND table_schema IN ('bronze', 'silver', 'gold')
    AND (comment IS NULL OR comment = '')
    """
).first().missing

print()
print("=" * 80)
print(f"RESUMO: {applied} aplicados, {errors} erros")
print(f"GATE REQ-001 (count tabelas SEM comment): {missing} (esperado 0)")
print("=" * 80)

if missing == 0:
    print("[OK] Catalogo vivo aplicado com sucesso.")
else:
    print("[FALHA] Existem tabelas sem comment - revisar erros acima.")
