from cuallee import Check
import pandas as pd


def test_positive(check: Check):
    check.is_in_billions("id")
    df = pd.DataFrame({"id": [1e9, 1e9 + 1]})
    assert check.validate(df).status.eq("PASS").all()


def test_negative(check: Check):
    check.is_in_billions("id")
    df = pd.DataFrame({"id": [1, 2]})
    assert check.validate(df).status.eq("FAIL").all()


def test_coverage(check: Check):
    check.is_in_billions("id", 0.5)
    df = pd.DataFrame({"id": [1.0, 1e9]})
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.5
