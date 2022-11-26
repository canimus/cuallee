import pandas as pd
from cuallee import Check
import pendulum as lu


def test_positive(check: Check):
    check.is_on_monday("id")
    df = pd.DataFrame(
        {"id": pd.Series([lu.now().next(lu.MONDAY).date()], dtype="datetime64[ns]")}
    )
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.is_on_monday("id")
    df = pd.DataFrame(
        {"id": pd.Series([lu.now().next(lu.TUESDAY).date()], dtype="datetime64[ns]")}
    )
    assert check.validate(df).status.str.match("FAIL").all()


def test_coverage(check: Check):
    check.is_on_monday("id", pct=1 / 7)
    df = (
        pd.date_range(start="2022-11-20", end="2022-11-26", freq="D")
        .rename("id")
        .to_frame()
        .reset_index(drop=True)
    )
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 1 / 7
