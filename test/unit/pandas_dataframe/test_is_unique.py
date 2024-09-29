import pandas as pd
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.is_unique("id")
    df = pd.DataFrame({"id": [10, 20, 30]})
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.is_unique("id")
    df = pd.DataFrame({"id": [10, 20, 30, 10]})
    assert check.validate(df).status.str.match("FAIL").all()


@pytest.mark.parametrize(
    "values",
    [[0, 1, 2], [0.1, 0.2, 0.0003], [1e2, 1e3, 1e4]],
    ids=("int", "float", "scientific"),
)
def test_parameters(check: Check, values):
    check.is_unique("id")
    df = pd.DataFrame({"id": values})
    result = check.validate(df)
    assert result.status.str.match("PASS").all()


def test_coverage(check: Check):
    check.is_unique("id", pct=3 / 4)
    df = pd.DataFrame({"id": [10, 20, 30, 10]})
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 3 / 4
