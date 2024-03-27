import daft
import pytest
import numpy as np

from cuallee import Check


def test_positive(check: Check):
    check.is_positive("id")
    df = daft.from_pydict({"id": np.arange(1, 10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.is_positive("id")
    df = daft.from_pydict({"id": [1, 2, 3, 4, -5]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


@pytest.mark.parametrize(
    "values", [[1, 2], [0.1, 0.2], [1e2, 1e3]], ids=("int", "float", "scientific")
)
def test_values(check: Check, values):
    check.is_positive("id")
    df = daft.from_pydict({"id": values})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_coverage(check: Check):
    check.is_positive("id", 0.5)
    df = daft.from_pydict({"id": [1, 2, -1, -2]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()
    assert result.select(daft.col("pass_rate").max() == 0.5).to_pandas().pass_rate.all()
