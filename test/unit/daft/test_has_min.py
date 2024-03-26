import daft
import pytest
import numpy as np

from cuallee import Check


def test_positive(check: Check):
    check.has_min("id", 0)
    df = daft.from_pydict({"id": np.arange(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.has_min("id", 5)
    df = daft.from_pydict({"id": np.arange(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


@pytest.mark.parametrize("extra_value", [0, 0.10001], ids=("int", "float"))
def test_values(check: Check, extra_value):
    check.has_min("id", extra_value)
    df = daft.from_pydict({"id": [1, 2, 3, 4] + [extra_value]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_min("id", 5, 0.1)
