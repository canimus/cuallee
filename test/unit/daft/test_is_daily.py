from datetime import datetime, timedelta

import daft
import pandas as pd

from cuallee import Check


def test_positive(check: Check):
    check.is_daily("id")
    pd_df = pd.DataFrame(
        {"id": pd.date_range(start="2022-01-01", end="2022-02-01", freq="D")}
    )
    df = daft.from_pandas(pd_df)
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.is_daily("id")
    pd_df = pd.DataFrame(
        {"id": [datetime.today() + timedelta(days=i) for i in range(1, 10, 2)]}
    )
    df = daft.from_pandas(pd_df)
    result = check.validate(df)
    # assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


def test_coverage(check: Check):
    check.is_daily("id", pct=0.6)
    pd_df = pd.DataFrame(
        {"id": [datetime(2022, 12, 12) + timedelta(days=i) for i in range(1, 10, 2)]}
    )
    df = daft.from_pandas(pd_df)
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()
    col_pass_rate = daft.col("pass_rate")
    assert (
        result.agg(col_pass_rate.max())
        .select(col_pass_rate == 0.60)
        .to_pandas()
        .pass_rate.all()
    )
