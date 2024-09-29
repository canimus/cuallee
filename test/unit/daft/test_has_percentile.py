import daft
import numpy as np
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.has_percentile("id", 6.75, 0.75)
    df = daft.from_pydict({"id": np.arange(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.has_percentile("id", 4.75, 0.75)
    df = daft.from_pydict({"id": np.arange(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


@pytest.mark.parametrize(
    "elements", [[1, 2, 3, 4, 5], [0.1, 0.2, 0.3, 0.4, 0.5]], ids=("int", "float")
)
def test_values(check: Check, elements):
    check.has_percentile("id", elements[2], 0.5)
    df = daft.from_pydict({"id": elements})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_percentile("id", 6.75, 0.8, 10000, 0.8)
