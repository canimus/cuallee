import pandas as pd
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.is_complete("id")
    df = pd.DataFrame({"id": [10, 20], "id2": [300, 500]})
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.is_complete("id")
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    assert check.validate(df).status.str.match("FAIL").all()


def test_coverage(check: Check):
    check.is_complete("id", 0.5)
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.5
