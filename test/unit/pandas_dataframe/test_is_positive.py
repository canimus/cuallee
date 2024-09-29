import numpy as np
import pandas as pd
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.is_positive("id")
    df = pd.DataFrame({"id": range(1, 10)})
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.is_positive("id")
    df = pd.DataFrame({"id": [1, 2, 3, 4, -5]})
    assert check.validate(df).status.str.match("FAIL").all()


@pytest.mark.parametrize(
    "values", [[1, 2], [0.1, 0.2], [1e2, 1e3]], ids=("int", "float", "scientific")
)
def test_values(check: Check, values):
    check.is_positive("id")
    df = pd.DataFrame({"id": values})
    assert check.validate(df).status.str.match("PASS").all()


def test_coverage(check: Check):
    check.is_positive("id", 0.5)
    df = pd.DataFrame({"id": [1, 2, -1, -2]})
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.5
