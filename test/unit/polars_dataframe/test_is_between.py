import polars as pl
from cuallee import Check
import pytest
from datetime import date


def test_positive(check: Check):
    check.is_between("id", (0, 9))
    df = pl.DataFrame({"id": range(10)})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.is_between("id", (100, 300))
    df = pl.DataFrame({"id": range(10)})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


@pytest.mark.parametrize(
    "rule_value, rule_data",
    [
        [(0, 9), range(10)],
        [
            (date(2022, 1, 1), date(2022, 1, 4)),
            pl.date_range(
                start=date(2022, 1, 1), end=date(2022, 1, 4), interval="1d", eager=True
            ),
        ],
    ],
    ids=("numeric", "date"),
)
def test_parameters(check: Check, rule_value, rule_data):
    check.is_between("id", rule_value)
    df = pl.DataFrame({"id": rule_data})

    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    check.is_between("id", (0, 10), 0.55)
    df = pl.DataFrame({"id": range(20)})
    result = check.validate(df)
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())
