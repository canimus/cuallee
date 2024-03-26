import daft
import pytest
import numpy as np

from cuallee import Check


def test_positive(check: Check):
    check.has_cardinality("id", 10)
    df = daft.from_pydict({"id": np.arange(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.has_cardinality("id", 5)
    df = daft.from_pydict({"id": np.arange(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_cardinality("id", 5, 0.1)
