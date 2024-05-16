import daft
import pytest
import numpy as np

from cuallee import Check


def test_positive(check: Check):
    check.is_equal_than("id", 1)
    df = daft.from_pydict({"id": np.repeat(1, 10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.is_equal_than("id", 5)
    df = daft.from_pydict({"id": np.arange(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


@pytest.mark.parametrize("extra_value", [1, 1.0], ids=("int", "float"))
def test_values(check: Check, extra_value):
    check.is_equal_than("id", extra_value)
    df = daft.from_pydict({"id": [1, 1, 1]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_coverage(check: Check):
    check.is_equal_than("id", 1, 0.75)
    df = daft.from_pydict({"id": [1, 1, 1, 0]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()
    col_pass_rate = daft.col("pass_rate")
    assert (
        result.agg(col_pass_rate.max()).select(col_pass_rate == 0.75).to_pandas().pass_rate.all()
    )

