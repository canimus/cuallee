import pandas as pd
from cuallee import Check
import pytest
import duckdb


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_workflow("group", "event", "order", [("x", "y"), ("y", "z"), ("z", None)])
    df = pd.DataFrame(
        {"group": list("AAABBB"), "event": list("xyzxyz"), "order": [1, 2, 3, 1, 2, 3]}
    )
    check.table_name = "df"
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_workflow("group", "event", "order", [("x", "y"), ("y", "z")])
    df = pd.DataFrame(
        {"group": list("AAABBB"), "event": list("xyzxyz"), "order": [1, 2, 3, 1, 2, 3]}
    )
    check.table_name = "df"
    assert check.validate(db).status.str.match("FAIL").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_workflow("group", "event", "order", [("x", "y"), ("y", "z")], pct=4 / 6)
    df = pd.DataFrame(
        {"group": list("AAABBB"), "event": list("xyzxyz"), "order": [1, 2, 3, 1, 2, 3]}
    )
    check.table_name = "df"
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 4 / 6
