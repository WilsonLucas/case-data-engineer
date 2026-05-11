"""Testes Camada 2 (chispa offline) para funcoes de notebooks/utils/data_helpers.py.

Cobre: parse de datas multi-formato, parse de timestamps multi-formato,
br_to_us_decimal e classificacao DQ por tipo. Cada teste constroi um
DataFrame in-memory via createDataFrame e valida o resultado via collect.
Sem dependencia de Hadoop/WinUtils — usa apenas operacoes em memoria.
"""

import datetime as dt
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from utils.data_helpers import (  # noqa: E402
    br_to_us_decimal,
    classify_dq_status,
    parse_multi_format_date,
    parse_multi_format_timestamp,
)


# -----------------------------------------------------------------------------
# parse_multi_format_date
# -----------------------------------------------------------------------------


def test_parse_date_iso_format(spark):
    df = spark.createDataFrame([("2025-01-15",)], ["d"])
    result = df.select(parse_multi_format_date("d").alias("parsed")).first()
    assert result.parsed == dt.date(2025, 1, 15)


def test_parse_date_br_format(spark):
    df = spark.createDataFrame([("15/01/2025",)], ["d"])
    result = df.select(parse_multi_format_date("d").alias("parsed")).first()
    assert result.parsed == dt.date(2025, 1, 15)


def test_parse_date_br_with_time(spark):
    df = spark.createDataFrame([("15/01/2025 10:30",)], ["d"])
    result = df.select(parse_multi_format_date("d").alias("parsed")).first()
    assert result.parsed == dt.date(2025, 1, 15)


def test_parse_date_invalid_returns_null(spark):
    df = spark.createDataFrame([("nao-eh-data",)], ["d"])
    result = df.select(parse_multi_format_date("d").alias("parsed")).first()
    assert result.parsed is None


# -----------------------------------------------------------------------------
# parse_multi_format_timestamp
# -----------------------------------------------------------------------------


def test_parse_timestamp_iso_with_t_separator(spark):
    df = spark.createDataFrame([("2025-01-15T10:30:00",)], ["ts"])
    result = df.select(parse_multi_format_timestamp("ts").alias("parsed")).first()
    assert result.parsed == dt.datetime(2025, 1, 15, 10, 30, 0)


def test_parse_timestamp_iso_with_space(spark):
    df = spark.createDataFrame([("2025-01-15 10:30:00",)], ["ts"])
    result = df.select(parse_multi_format_timestamp("ts").alias("parsed")).first()
    assert result.parsed == dt.datetime(2025, 1, 15, 10, 30, 0)


def test_parse_timestamp_invalid_returns_null(spark):
    df = spark.createDataFrame([("xyz",)], ["ts"])
    result = df.select(parse_multi_format_timestamp("ts").alias("parsed")).first()
    assert result.parsed is None


# -----------------------------------------------------------------------------
# br_to_us_decimal
# -----------------------------------------------------------------------------


def test_decimal_br_with_comma(spark):
    df = spark.createDataFrame([("1234,56",)], ["v"])
    result = df.select(br_to_us_decimal("v").alias("parsed")).first()
    assert result.parsed == Decimal("1234.56")


def test_decimal_already_us(spark):
    df = spark.createDataFrame([("1234.56",)], ["v"])
    result = df.select(br_to_us_decimal("v").alias("parsed")).first()
    assert result.parsed == Decimal("1234.56")


def test_decimal_invalid_returns_null(spark):
    df = spark.createDataFrame([("N/A",)], ["v"])
    result = df.select(br_to_us_decimal("v").alias("parsed")).first()
    assert result.parsed is None


def test_decimal_empty_returns_null(spark):
    df = spark.createDataFrame([("",)], ["v"])
    result = df.select(br_to_us_decimal("v").alias("parsed")).first()
    assert result.parsed is None


# -----------------------------------------------------------------------------
# classify_dq_status (regra-chave do pipeline DQ)
# -----------------------------------------------------------------------------


def test_dq_clean_when_no_reasons(spark):
    df = spark.createDataFrame([(["", ""],)], ["_dq_reasons"]).withColumn(
        "_dq_reasons", F.array().cast("array<string>")
    )
    result = df.select(classify_dq_status().alias("status")).first()
    assert result.status == "clean"


def test_dq_warning_with_2_reasons(spark):
    df = spark.createDataFrame([(["formato data invalido", "enum nao canonico"],)], ["_dq_reasons"])
    result = df.select(classify_dq_status().alias("status")).first()
    assert result.status == "warning"


def test_dq_rejected_when_pk_ausente(spark):
    df = spark.createDataFrame([(["PK ausente"],)], ["_dq_reasons"])
    result = df.select(classify_dq_status().alias("status")).first()
    assert result.status == "rejected"


def test_dq_rejected_when_many_reasons(spark):
    df = spark.createDataFrame([(["a", "b", "c", "d"],)], ["_dq_reasons"])
    result = df.select(classify_dq_status().alias("status")).first()
    assert result.status == "rejected"
