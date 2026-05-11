"""Funcoes utilitarias compartilhadas entre os notebooks silver e gold.

Extraidas para reuso e testabilidade. Todas as funcoes recebem o NOME da coluna
como string (nao Column), porque usam F.expr() internamente para tirar proveito
de funcoes ANSI-safe (try_cast, try_to_date, try_to_timestamp) que nao estao
expostas como wrappers Python no Photon Spark 4.1 (Free Edition).

Padrao de retorno: pyspark.sql.Column. Composavel com .withColumn() e .select().
"""

from typing import List, Optional

from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.dataframe import DataFrame


def parse_multi_format_date(col_name: str) -> Column:
    """Tenta parsear uma string de data em multiplos formatos comuns.

    Tenta na ordem: ISO 8601 (yyyy-MM-dd), formato BR (dd/MM/yyyy) e formato
    BR com hora (dd/MM/yyyy HH:mm) — neste ultimo, descarta o tempo. Retorna
    NULL se nenhum formato casa, sem lancar excecao (proteccao ANSI mode).

    Args:
        col_name: Nome da coluna string contendo a data.

    Returns:
        Column do tipo DATE com o valor parseado ou NULL.

    Example:
        >>> df.withColumn("order_date", parse_multi_format_date("order_date"))
    """
    return F.coalesce(
        F.expr(f"try_to_date({col_name}, 'yyyy-MM-dd')"),
        F.expr(f"try_to_date({col_name}, 'dd/MM/yyyy')"),
        F.expr(f"try_to_timestamp({col_name}, 'dd/MM/yyyy HH:mm')").cast("date"),
    )


def parse_multi_format_timestamp(col_name: str) -> Column:
    """Tenta parsear uma string de timestamp em multiplos formatos comuns.

    Trata o caso ISO 8601 com 'T' literal substituindo por espaco antes do
    parse, dado que o DateTimeFormatter do Spark exige escape complicado para
    'T' literal. Retorna NULL se nenhum formato casa.

    Args:
        col_name: Nome da coluna string contendo o timestamp.

    Returns:
        Column do tipo TIMESTAMP com o valor parseado ou NULL.

    Example:
        >>> df.withColumn("created_at", parse_multi_format_timestamp("created_at"))
    """
    return F.coalesce(
        F.expr(f"try_to_timestamp(replace({col_name}, 'T', ' '), 'yyyy-MM-dd HH:mm:ss')"),
        F.expr(f"try_to_timestamp({col_name}, 'yyyy-MM-dd HH:mm:ss')"),
        F.expr(f"try_to_timestamp({col_name}, 'yyyy-MM-dd')"),
        F.expr(f"try_to_timestamp({col_name}, 'dd/MM/yyyy HH:mm')"),
        F.expr(f"try_to_timestamp({col_name}, 'dd/MM/yyyy')"),
    )


def br_to_us_decimal(col_name: str, precision: int = 15, scale: int = 2) -> Column:
    """Converte string com decimal estilo BR para DECIMAL US.

    Substitui virgula por ponto e usa try_cast (ANSI-safe) — valores nao-numericos
    como 'N/A', '-' ou string vazia viram NULL ao inves de lancar
    CAST_INVALID_INPUT (default em Photon Spark 4.1 ANSI mode estrito).

    Args:
        col_name: Nome da coluna string contendo o decimal.
        precision: Precisao total do decimal alvo. Default 15.
        scale: Escala do decimal alvo. Default 2.

    Returns:
        Column do tipo DECIMAL(precision, scale) ou NULL.

    Example:
        >>> df.withColumn("net_amount", br_to_us_decimal("net_amount"))
    """
    return F.expr(f"try_cast(replace({col_name}, ',', '.') as decimal({precision},{scale}))")


def profile_dataframe(df: DataFrame, sample_size: int = 5) -> dict:
    """Gera um perfil descritivo do DataFrame: schema, contagem, amostra, nulls.

    Util para celulas de exploracao em notebooks de inspecao. Nao assume nada
    sobre o conteudo — funciona em DataFrames de qualquer schema.

    Args:
        df: DataFrame a perfilar.
        sample_size: Quantas linhas mostrar como amostra. Default 5.

    Returns:
        Dict com chaves: total_rows, total_cols, columns (lista),
        null_counts (dict por coluna), sample (lista de Rows).
    """
    total_rows = df.count()
    columns = df.columns
    null_counts = {}
    for col in columns:
        null_counts[col] = df.filter(F.col(col).isNull()).count()
    sample = df.limit(sample_size).collect()
    return {
        "total_rows": total_rows,
        "total_cols": len(columns),
        "columns": columns,
        "null_counts": null_counts,
        "sample": sample,
    }


def classify_dq_status(reasons_col: str = "_dq_reasons", critical_substrings: Optional[List[str]] = None) -> Column:
    """Classifica DQ status com base no array _dq_reasons.

    Estrategia por TIPO (nao por contagem): se qualquer razao contem uma
    substring critica (ex: 'PK ausente', 'FK ausente'), marca como rejected
    independentemente da contagem total. Caso contrario aplica regra simples
    por tamanho. Substitui o pattern problematico de classificar so por size.

    Args:
        reasons_col: Nome da coluna ARRAY<STRING> com as razoes. Default '_dq_reasons'.
        critical_substrings: Substrings que indicam issue critica. Default
            inclui PK/FK ausente.

    Returns:
        Column STRING com valor 'clean', 'warning' ou 'rejected'.
    """
    if critical_substrings is None:
        critical_substrings = ["PK ausente", "FK ausente", "obrigatorio"]

    has_critical = F.lit(False)
    for sub in critical_substrings:
        has_critical = has_critical | F.array_contains(F.col(reasons_col), sub)

    return (
        F.when(has_critical, F.lit("rejected"))
        .when(F.size(reasons_col) == 0, F.lit("clean"))
        .when(F.size(reasons_col) <= 2, F.lit("warning"))
        .otherwise(F.lit("rejected"))
    )
