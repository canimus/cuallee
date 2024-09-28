import pandas as pd
from cuallee import Check
import pytest
import duckdb


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.satisfies("id", "(id > 0) AND (id2 > 200)")
    df = pd.DataFrame({"id": [10, 20], "id2": [300, 500]})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.satisfies(("id", "id2"), "(id < 0) AND (id2 > 1000)")
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("FAIL").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.satisfies("id", "id > 0", pct=0.5)
    df = pd.DataFrame({"id": [10, -10], "id2": [300, 500]})
    check.table_name = "df"
    db.register("df", df)
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.5
