import pandas as pd

from cuallee import Check


def test_positive(check: Check):
    check.is_in_millions("id")
    df = pd.DataFrame({"id": [1e6, 1e6 + 1]})
    assert check.validate(df).status.eq("PASS").all()


def test_negative(check: Check):
    check.is_in_millions("id")
    df = pd.DataFrame({"id": [1, 2]})
    assert check.validate(df).status.eq("FAIL").all()


def test_coverage(check: Check):
    check.is_in_millions("id", 0.5)
    df = pd.DataFrame({"id": [1.0, 1e6]})
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.5
