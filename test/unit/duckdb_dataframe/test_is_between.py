import pandas as pd
from cuallee import Check
import pytest
import duckdb


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_between("id", (0, 9))
    df = pd.DataFrame({"id": range(10)})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_between("id", (100, 300))
    df = pd.DataFrame({"id": range(10)})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("FAIL").all()


@pytest.mark.parametrize(
    "rule_value, rule_data",
    [
        [(0, 9), range(10)],
        [
            ("2022-01-01", "2022-02-02"),
            pd.date_range(start="2022-01-01", end="2022-02-01", freq="D").strftime(
                "%Y-%m-%d"
            ),
        ],
    ],
    ids=("numeric", "date"),
)
def test_parameters(check: Check, db: duckdb.DuckDBPyConnection, rule_value, rule_data):
    check.is_between("id", rule_value)
    df = pd.DataFrame({"id": rule_data})
    check.table_name = "df"
    db.register("df", df)
    result = check.validate(db)
    assert result.status.str.match("PASS").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.is_between("id", (0, 10), 0.55)
    df = pd.DataFrame({"id": range(20)})
    check.table_name = "df"
    db.register("df", df)
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.55
