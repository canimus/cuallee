from datetime import datetime, timedelta

import pandas as pd
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.is_on_schedule("id", (9, 17))
    df = pd.DataFrame(
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
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.is_on_schedule("id", (9, 17))
    df = pd.DataFrame(
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

    assert check.validate(df).status.str.match("FAIL").all()


def test_coverage(check: Check):
    check.is_on_schedule("id", (9, 17), pct=7 / 8)
    df = pd.DataFrame(
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
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 7 / 8
