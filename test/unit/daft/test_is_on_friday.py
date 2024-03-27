import daft
import pandas as pd
import pendulum as lu

from cuallee import Check


def test_positive(check: Check):
    check.is_on_friday("id")
    pd_df = pd.DataFrame(
        {"id": pd.Series([lu.now().next(lu.FRIDAY).date()], dtype="datetime64[ns]")}
    )
    df = daft.from_pandas(pd_df)
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.is_on_friday("id")
    pd_df = pd.DataFrame(
        {"id": pd.Series([lu.now().next(lu.MONDAY).date()], dtype="datetime64[ns]")}
    )
    df = daft.from_pandas(pd_df)
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


def test_coverage(check: Check):
    check.is_on_friday("id", pct=1 / 7)
    pd_df = (
        pd.date_range(start="2022-11-20", end="2022-11-26", freq="D")
        .rename("id")
        .to_frame()
        .reset_index(drop=True)
    )
    df = daft.from_pandas(pd_df)
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()
    assert result.select(daft.col("pass_rate").max() == 1 / 7).to_pandas().pass_rate.all()