import daft
import pandas as pd

from cuallee import Check


def test_positive(check: Check):
    check.is_on_schedule("id", (9, 17))
    pd_df = pd.DataFrame(
        {
            "id": pd.Series(
                [
                    "2022-01-01T09:01:00",
                    "2022-01-01T10:01:00",
                    "2022-01-01T11:01:00",
                    "2022-01-01T12:01:00",
                    "2022-01-01T13:01:00",
                    "2022-01-01T14:01:00",
                    "2022-01-01T15:01:00",
                    "2022-01-01T16:01:00",
                ],
                dtype="datetime64[ns]",
            )
        }
    )
    df = daft.from_pandas(pd_df)
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.is_on_schedule("id", (9, 17))
    pd_df = pd.DataFrame(
        {
            "id": pd.Series(
                [
                    "2022-01-01T09:01:00",
                    "2022-01-01T10:01:00",
                    "2022-01-01T11:01:00",
                    "2022-01-01T12:01:00",
                    "2022-01-01T13:01:00",
                    "2022-01-01T14:01:00",
                    "2022-01-01T15:01:00",
                    "2022-01-01T21:01:00",
                ],
                dtype="datetime64[ns]",
            )
        }
    )

    df = daft.from_pandas(pd_df)
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


def test_coverage(check: Check):
    check.is_on_schedule("id", (9, 17), pct=7 / 8)
    pd_df = pd.DataFrame(
        {
            "id": pd.Series(
                [
                    "2022-01-01T09:01:00",
                    "2022-01-01T10:01:00",
                    "2022-01-01T11:01:00",
                    "2022-01-01T12:01:00",
                    "2022-01-01T13:01:00",
                    "2022-01-01T14:01:00",
                    "2022-01-01T15:01:00",
                    "2022-01-01T21:01:00",
                ],
                dtype="datetime64[ns]",
            )
        }
    )
    df = daft.from_pandas(pd_df)
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()

    col_pass_rate = daft.col("pass_rate")
    assert (
        result.agg(col_pass_rate.max())
        .select(col_pass_rate == 7 / 8)
        .to_pandas()
        .pass_rate.all()
    )
