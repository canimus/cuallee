import pytest

from cuallee import Check
import pandas as pd
import numpy as np
import duckdb


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_correlation("id", "id2", 1.0)
    df = pd.DataFrame({"id": [10, 20], "id2": [100, 200]})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_correlation("id", "id2", 1.0)
    df = pd.DataFrame({"id": [10, 20, 30, None], "id2": [100, 200, 300, 400]})
    check.table_name = "df"
    assert check.validate(db).status.eq("FAIL").all()


def test_values(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_correlation("id", "id2", 1.0)
    df = pd.DataFrame({"id": [1, 2, 3], "id2": [1.0, 2.0, 3.0]})
    check.table_name = "df"
    assert check.validate(db).status.eq("PASS").all()


def test_coverage(check: Check):
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_correlation("id", "id2", 1.0, 0.75)
