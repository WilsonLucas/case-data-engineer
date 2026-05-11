"""Constantes de configuracao do pipeline Levva.

Centraliza schemas, paths do Volume, formatos de data validos e lookups canonicos.
Importado pelos notebooks bronze/silver/gold para evitar duplicacao.
"""

# Schemas UC (decisao 10 do DESIGN — sem prefixo case_levva_)
CATALOG = "workspace"
LANDING_SCHEMA = f"{CATALOG}.landing"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"

# Path do Volume com arquivos brutos
SOURCES_BASE = f"/Volumes/{CATALOG}/landing/sources"

# Formatos de data aceitos no parsing multi-formato (silver)
DATE_FORMATS = [
    "yyyy-MM-dd",
    "dd/MM/yyyy",
]

TIMESTAMP_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd",
    "dd/MM/yyyy HH:mm",
    "dd/MM/yyyy",
]

# Tags UC fixas aplicadas pelo notebook de governanca
GOVERNANCE_TAGS = {
    "owner": "wilsonlucas201@gmail.com",
    "classification": "internal",
}

# Lookup canonico de status de pedido (silver_pedidos)
STATUS_CANONICAL = {
    "FATURADO": "FATURADO",
    "EM_SEPARACAO": "EM_SEPARACAO",
    "EM SEPARACAO": "EM_SEPARACAO",
    "SEPARANDO": "EM_SEPARACAO",
    "CANCELADO": "CANCELADO",
    "CANCELLED": "CANCELADO",
}

# Lookup canonico de regiao (silver_regioes)
REGIAO_CANONICAL = {
    "S": "SUL",
    "SUL": "SUL",
    "SE": "SUDESTE",
    "SUDESTE": "SUDESTE",
    "NE": "NORDESTE",
    "NORDESTE": "NORDESTE",
    "N": "NORTE",
    "NORTE": "NORTE",
    "CO": "CENTRO-OESTE",
    "CENTRO-OESTE": "CENTRO-OESTE",
}

# Feriados nacionais BR 2025 (12 oficiais, exclui Carnaval que e ponto facultativo)
FERIADOS_BR_2025 = {
    "2025-01-01": "Confraternizacao Universal",
    "2025-04-18": "Sexta-feira Santa",
    "2025-04-21": "Tiradentes",
    "2025-05-01": "Dia do Trabalho",
    "2025-06-19": "Corpus Christi",
    "2025-09-07": "Independencia do Brasil",
    "2025-10-12": "Nossa Senhora Aparecida",
    "2025-11-02": "Finados",
    "2025-11-15": "Proclamacao da Republica",
    "2025-11-20": "Dia da Consciencia Negra",
    "2025-12-25": "Natal",
}

# Schema de severity para ocorrencias (silver_ocorrencias)
SEVERITY_SCORE = {
    "HIGH": 3,
    "MEDIUM": 2,
    "LOW": 1,
}
