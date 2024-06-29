import pandas as pd
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.is_empty("id")
    df = pd.DataFrame({"id": [None, None], "id2": [None, None]})
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.is_empty("id")
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    assert check.validate(df).status.str.match("FAIL").all()


def test_coverage(check: Check):
    check.is_empty("id2", 0.5)
    df = pd.DataFrame({"id": [10, None], "id2": [None, "test"]})
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
