import pandas as pd
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.has_entropy("id", 1.0)
    df = pd.DataFrame({"id": [1, 1, 1, 0, 0, 0]})
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.has_entropy("id", 1.0)
    df = pd.DataFrame({"id": [10, 10, 10, 10, 50]})
    assert check.validate(df).status.str.match("FAIL").all()


@pytest.mark.parametrize(
    "values", [[1], [1, 1, 1, 1, 1]], ids=("observation", "classes")
)
def test_parameters(check: Check, values):
    check.has_entropy("id", 0.0)
    df = pd.DataFrame({"id": values})
    assert check.validate(df).status.str.match("PASS").all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_entropy("id", 1.0, pct=0.5)
