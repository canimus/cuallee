import daft
import pytest
import numpy as np

from cuallee import Check


def test_positive(check: Check):
    check.has_workflow("group", "event", "order", [("x", "y"), ("y", "z"), ("z", None)])
    df = daft.from_pydict(
        {"group": list("AAABBB"), "event": list("xyzxyz"), "order": [1, 2, 3, 1, 2, 3]}
    )
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.has_workflow("group", "event", "order", [("x", "y"), ("y", "z")])
    df = daft.from_pydict(
        {"group": list("AAABBB"), "event": list("xyzxyz"), "order": [1, 2, 3, 1, 2, 3]}
    )
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


def test_coverage(check: Check):
    check.has_workflow("group", "event", "order", [("x", "y"), ("y", "z")], pct=4 / 6)
    df = daft.from_pydict(
        {"group": list("AAABBB"), "event": list("xyzxyz"), "order": [1, 2, 3, 1, 2, 3]}
    )
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()
    assert result.select(daft.col("pass_rate").max() == 4 / 6).to_pandas().pass_rate.all()

