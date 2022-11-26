import pandas as pd
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.satisfies("id", "(id > 0) & (id2 > 200)")
    df = pd.DataFrame({"id": [10, 20], "id2": [300, 500]})
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.satisfies(("id", "id2"), "(id < 0) & (id2 > 1000)")
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    assert check.validate(df).status.str.match("FAIL").all()


def test_coverage(check: Check):
    check.satisfies("id", "id > 0", pct=0.5)
    df = pd.DataFrame({"id": [10, -10], "id2": [300, 500]})
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.5
