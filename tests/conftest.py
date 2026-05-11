"""Fixtures pytest compartilhadas.

A fixture `spark` cria SparkSession local em modo single-thread (`local[1]`),
suficiente para testes de transformacoes que usam `createDataFrame` +
`collect()`. Sem dependencia de WinUtils/Hadoop.

Em Windows, se PySpark falhar com erro relacionado a Hadoop, a fixture eh
marcada como skip e os testes Camada 2 sao pulados; testes Camada 1
(Python puro em test_data_helpers_python.py) continuam rodando.
"""

import sys
from typing import Optional

import pytest

_spark_session: Optional[object] = None


@pytest.fixture(scope="session")
def spark():
    """SparkSession local single-thread para testes de transformacoes.

    Yields:
        pyspark.sql.SparkSession configurada com master local[1] e ANSI mode
        ativo (paridade com Photon Spark 4.1 em Free Edition).
    """
    global _spark_session
    if _spark_session is not None:
        return _spark_session

    try:
        from pyspark.sql import SparkSession

        _spark_session = (
            SparkSession.builder.master("local[1]")
            .appName("case-levva-tests")
            .config("spark.sql.ansi.enabled", "true")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        _spark_session.sparkContext.setLogLevel("ERROR")
        return _spark_session
    except Exception as e:
        pytest.skip(f"Spark local nao disponivel neste ambiente ({type(e).__name__}): {e}")


@pytest.fixture(scope="session", autouse=True)
def _add_notebooks_to_path():
    """Adiciona notebooks/ ao sys.path para permitir 'from utils.X import Y'."""
    import os

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    notebooks_path = os.path.join(repo_root, "notebooks")
    if notebooks_path not in sys.path:
        sys.path.insert(0, notebooks_path)
    yield
