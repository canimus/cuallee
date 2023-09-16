import pandas as pd
import numpy as np
from cuallee import Check
import pytest
import duckdb


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_pattern("id", r"^H.*")
    df = pd.DataFrame({"id": ["Herminio", "Hola", "Heroe"]})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_pattern("id", r"^H.*")
    df = pd.DataFrame({"id": ["Herminio", "Hola", "Villain"]})
    check.table_name = "df"
    assert check.validate(db).status.str.match("FAIL").all()


@pytest.mark.parametrize(
    "pattern", ["^H.*", "^H.*"], ids=("single_quote", "double_quote")
)
def test_values(check: Check, db: duckdb.DuckDBPyConnection, pattern):
    check.has_pattern("id", pattern)
    df = pd.DataFrame({"id": ["Herminio", "Hola", "Heroe"]})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_pattern("id", r"^H.*", 0.75)
    df = pd.DataFrame({"id": ["Herminio", "Hola", "Villain", "Heroe"]})
    check.table_name = "df"
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.75
