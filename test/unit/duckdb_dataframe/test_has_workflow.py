import pandas as pd
from cuallee import Check
import pytest
import duckdb


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_workflow("name", "event", "ord", [("x", "y"), ("y", "z"), ("z", None)])
    df = pd.DataFrame(
        {"name": list("AAABBB"), "event": list("xyzxyz"), "ord": [1, 2, 3, 1, 2, 3]}
    )
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_parameters(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_workflow("X", "Y", "Z", [("x", "y"), ("y", "z"), ("z", None)])
    check.is_complete("X")
    df = pd.DataFrame(
        {"X": list("AAABBB"), "Y": list("xyzxyz"), "Z": [1, 2, 3, 1, 2, 3]}
    )
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_workflow("name", "event", "ord", [("x", "y"), ("y", "z")])
    df = pd.DataFrame(
        {"name": list("AAABBB"), "event": list("xyzxyz"), "ord": [1, 2, 3, 1, 2, 3]}
    )
    check.table_name = "df"
    db.register("df", df)
    assert check.validate(db).status.str.match("FAIL").all()


def test_coverage(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_workflow("name", "event", "ord", [("x", "y"), ("y", "z")], pct=4 / 6)
    df = pd.DataFrame(
        {"name": list("AAABBB"), "event": list("xyzxyz"), "ord": [1, 2, 3, 1, 2, 3]}
    )
    check.table_name = "df"
    db.register("df", df)
    result = check.validate(db)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 4 / 6
