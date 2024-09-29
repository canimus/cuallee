import duckdb
import pandas as pd

from cuallee import Check


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_in_billions("id")
    df = pd.DataFrame({"id": [1e9, 1e9 + 1]})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.eq("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_in_billions("id")
    df = pd.DataFrame({"id": [1, 2]})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.eq("FAIL").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_in_billions("id", 0.5)
    df = pd.DataFrame({"id": [1.0, 1e9]})
    check.table_name = "df"
    db.register("df", df)
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.5
