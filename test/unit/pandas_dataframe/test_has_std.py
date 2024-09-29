import numpy as np
import pandas as pd
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.has_std("id", 3.0276503540974917)
    df = pd.DataFrame({"id": np.arange(10)})
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.has_std("id", 5)
    df = pd.DataFrame({"id": np.arange(10)})
    assert check.validate(df).status.str.match("FAIL").all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_std("id", 5, 0.1)
