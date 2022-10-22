
from cuallee import Check
import pandas as pd
import numpy as np

def test_numeric_columns(check: Check):
    check.has_correlation("id", "id2", 1.0)
    df = pd.DataFrame({"id": [10, 20], "id2": [100, 200]})
    assert check.validate(df).status.str.match("PASS").all()

def test_large_matrix(check: Check):
    check.has_correlation("id", "id2", 1.0)
    df = pd.DataFrame({"id":np.arange(100), "id2" : np.arange(100)*10})
    assert check.validate(df).status.eq("PASS").all()

def test_empty_columns(check: Check):
    check.has_correlation("id", "id2", 1.0)
    df = pd.DataFrame({"id": [10,20,30, None], "id2": [100, 200, 300, 400]})
    assert check.validate(df).status.eq("PASS").all()


def test_coverage(check: Check):
    check.has_correlation("id", "id2", 1.0, 0.75)
    df = pd.DataFrame({"id": [10, None], "id2": [100, 200]})
    result = check.validate(df)
    assert result.status.str.match("FAIL").all()
    assert result.pass_rate.max() == 0.0
