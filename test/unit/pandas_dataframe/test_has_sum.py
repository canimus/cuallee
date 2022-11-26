import pandas as pd
import numpy as np
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.has_sum("id", 45)
    df = pd.DataFrame({"id": np.arange(10)})
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.has_sum("id", 5)
    df = pd.DataFrame({"id": np.arange(10)})
    assert check.validate(df).status.str.match("FAIL").all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_sum("id", 5, 0.1)
