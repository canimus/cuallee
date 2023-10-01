import polars as pl
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.has_workflow("group", "event", "order", [("x", "y"), ("y", "z"), ("z", None)])
    df = pl.DataFrame(
        {"group": list("AAABBB"), "event": list("xyzxyz"), "order": [1, 2, 3, 1, 2, 3]}
    )
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.has_workflow("group", "event", "order", [("x", "y"), ("y", "z")])
    df = pl.DataFrame(
        {"group": list("AAABBB"), "event": list("xyzxyz"), "order": [1, 2, 3, 1, 2, 3]}
    )
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    check.has_workflow("group", "event", "order", [("x", "y"), ("y", "z")], pct=4 / 6)
    df = pl.DataFrame(
        {"group": list("AAABBB"), "event": list("xyzxyz"), "order": [1, 2, 3, 1, 2, 3]}
    )
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())
