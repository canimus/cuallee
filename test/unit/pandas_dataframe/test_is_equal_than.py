import pandas as pd
import numpy as np
from cuallee import Check
import pytest


def test_positve(check: Check):
    check.is_equal_than("id", 1)
    df = pd.DataFrame({"id": np.repeat(1, 10)})
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.is_equal_than("id", 5)
    df = pd.DataFrame({"id": np.arange(10)})
    assert check.validate(df).status.str.match("FAIL").all()


@pytest.mark.parametrize("extra_value", [1, 1.0], ids=("int", "float"))
def test_values(check: Check, extra_value):
    check.is_equal_than("id", extra_value)
    df = pd.DataFrame({"id": [1, 1, 1]})
    assert check.validate(df).status.str.match("PASS").all()


def test_coverage(check: Check):
    check.is_equal_than("id", 1, 0.75)
    df = pd.DataFrame({"id": [1, 1, 1, 0]})
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.75
