import pandas as pd
from cuallee import Check
import pendulum as lu
import duckdb


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_on_weekday("id")
    df = pd.DataFrame(
        {"id": pd.Series([lu.now().next(lu.WEDNESDAY).date()], dtype="datetime64[ns]")}
    )
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_on_weekday("id")
    df = pd.DataFrame(
        {"id": pd.Series([lu.now().next(lu.SATURDAY).date()], dtype="datetime64[ns]")}
    )
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("FAIL").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_on_weekday("id", pct=5 / 7)
    df = (
        pd.date_range(start="2022-11-20", end="2022-11-26", freq="D")
        .rename("id")
        .to_frame()
        .reset_index(drop=True)
    )
    check.table_name = "df"
    db.register("df", df)
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 5 / 7
