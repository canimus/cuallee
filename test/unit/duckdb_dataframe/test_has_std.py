import pandas as pd
import numpy as np
from cuallee import Check
import pytest
import duckdb


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_std("id", 3.0276503540974917)
    df = pd.DataFrame({"id": np.arange(10)})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_std("id", 5)
    df = pd.DataFrame({"id": np.arange(10)})
    check.table_name = "df"
    assert check.validate(db).status.str.match("FAIL").all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_std("id", 5, 0.1)
