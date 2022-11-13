import pandas as pd
import numpy as np
from cuallee import Check
import pytest


def test_positve(check: Check):
    check.has_max("id", 9)
    df = pd.DataFrame({"id": np.arange(10)})
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.has_max("id", 5)
    df = pd.DataFrame({"id": np.arange(10)})
    assert check.validate(df).status.str.match("FAIL").all()


@pytest.mark.parametrize("extra_value", [10, 10001.10001], ids=("int", "float"))
def test_values(check: Check, extra_value):
    check.has_max("id", extra_value)
    df = pd.DataFrame({"id": [0, 1, 2, 3, 4] + [extra_value]})
    assert check.validate(df).status.str.match("PASS").all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_max("id", 5, 0.1)
