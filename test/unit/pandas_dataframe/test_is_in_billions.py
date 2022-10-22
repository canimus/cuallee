from cuallee import Check
import pandas as pd


def test_in_range(check: Check):
    check.is_in_billions("id")
    df = pd.DataFrame({"id": [1e9, 1e9 + 1]})
    assert check.validate(df).status.eq("PASS").all()


def test_below_range(check: Check):
    check.is_in_billions("id")
    df = pd.DataFrame({"id": [1, 2]})
    assert check.validate(df).status.eq("FAIL").all()


def test_coverage(check: Check):
    check.is_in_billions("id", 0.5)
    df = pd.DataFrame({"id": [1.0, 1e9]})
    assert check.validate(df).status.eq("PASS").all()
