from cuallee import Check
import pandas as pd
import pytest
import duckdb


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.are_unique(("id", "id2"))
    df = pd.DataFrame({"id": [10, 20], "id2": [300, 500]})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.are_unique(("id", "id2"))
    df = pd.DataFrame({"id": [10, 10], "id2": [300, 300]})
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("FAIL").all()


@pytest.mark.parametrize(
    "rule_column", [tuple(["id", "id2"]), list(["id", "id2"])], ids=("tuple", "list")
)
def test_parameters(check: Check, db: duckdb.DuckDBPyConnection, rule_column):
    check.are_unique(rule_column)
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    check.table_name = "df"
    db.register("df", df)
    result = check.validate(db)
    assert result.status.str.match("FAIL").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.are_unique(("id", "id2"), 0.75)
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    check.table_name = "df"
    db.register("df", df)
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.75
