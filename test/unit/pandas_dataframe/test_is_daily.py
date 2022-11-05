import pandas as pd
from cuallee import Check
import pytest
from datetime import datetime, timedelta


def test_positive(check: Check):
    check.is_daily("id")
    df = pd.DataFrame(
        {"id": pd.date_range(start="2022-01-01", end="2022-02-01", freq="D")}
    )
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.is_daily("id")
    df = pd.DataFrame(
        {"id": [datetime.today() + timedelta(days=i) for i in range(1, 10, 2)]}
    )
    assert check.validate(df).status.str.match("FAIL").all()


def test_coverage(check: Check):
    check.is_daily("id", pct=0.4)
    df = pd.DataFrame(
        {"id": [datetime.today() + timedelta(days=i) for i in range(1, 10, 2)]}
    )
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.4
