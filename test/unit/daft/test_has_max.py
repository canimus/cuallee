import daft
import numpy as np
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.has_max("id", 9)
    df = daft.from_pydict({"id": np.arange(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.has_max("id", 5)
    df = daft.from_pydict({"id": np.arange(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


@pytest.mark.parametrize("extra_value", [10, 10001.10001], ids=("int", "float"))
def test_values(check: Check, extra_value):
    check.has_max("id", extra_value)
    df = daft.from_pydict({"id": [0, 1, 2, 3, 4] + [extra_value]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_max("id", 5, 0.1)
