import daft
import pytest
import numpy as np
import pandas as pd

from cuallee import Check


def test_positive(check: Check):
    check.is_between("id", (0, 9))
    df = daft.from_pydict({"id": np.arange(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.is_between("id", (100, 300))
    df = daft.from_pydict({"id": np.arange(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


@pytest.mark.parametrize(
    "rule_value, rule_data",
    [
        [(0, 9), np.arange(10)],
        [
            ("2022-01-01", "2022-02-02"),
            pd.date_range(start="2022-01-01", end="2022-02-01", freq="D")
            .strftime("%Y-%m-%d")
            .to_list(),
        ],
    ],
    ids=("numeric", "date"),
)
def test_parameters(check: Check, rule_value, rule_data):
    check.is_between("id", rule_value)
    df = daft.from_pydict({"id": rule_data})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_coverage(check: Check):
    check.is_between("id", (0, 10), 0.55)
    df = daft.from_pydict({"id": np.arange(20)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()

    col_pass_rate = daft.col("pass_rate")
    assert (
        result.agg(col_pass_rate.max())
        .select(col_pass_rate == 0.55)
        .to_pandas()
        .pass_rate.all()
    )
