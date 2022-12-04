import duckdb
from cuallee import Check
import pytest
import pandas as pd


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.are_complete(("id", "id2"))
    df = pd.DataFrame({"id": [10, 20], "id2": [300, 500]})
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.are_complete(("id", "id2"))
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    check.table_name = "df"
    assert check.validate(db).status.str.match("FAIL").all()


@pytest.mark.parametrize(
    "rule_column", [tuple(["id", "id2"]), list(["id", "id2"])], ids=("tuple", "list")
)
def test_parameters(check: Check, db: duckdb.DuckDBPyConnection, rule_column):
    check.are_complete(rule_column)
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    check.table_name = "df"
    result = check.validate(db)
    assert result.status.str.match("FAIL").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.are_complete(("id", "id2"), 0.75)
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    check.table_name = "df"
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
