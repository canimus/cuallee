import pandas as pd
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.has_entropy("id", 1.0)
    df = pd.DataFrame({"id": [1, 1, 1, 0, 0, 0]})
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.has_entropy("id")
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    assert check.validate(df).status.str.match("FAIL").all()


def test_coverage(check: Check):
    check.has_entropy("id", 0.5)
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.5
