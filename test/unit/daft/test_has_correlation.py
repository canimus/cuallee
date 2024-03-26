import daft
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.has_correlation("id", "id2", 1.0)
    df = daft.from_pydict({"id": [10, 20], "id2": [100, 200]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.has_correlation("id", "id2", 1.0)
    df = daft.from_pydict({"id": [10, 20, 30, None], "id2": [100, 200, 300, 400]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_values(check: Check):
    check.has_correlation("id", "id2", 1.0)
    df = daft.from_pydict({"id": [1, 2, 3], "id2": [1.0, 2.0, 3.0]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_coverage(check: Check):
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_correlation("id", "id2", 1.0, 0.75)
