import polars as pl
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.has_entropy("id", 1.0)
    df = pl.DataFrame({"id": [1, 1, 1, 0, 0, 0]})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.has_entropy("id", 1.0)
    df = pl.DataFrame({"id": [10, 10, 10, 10, 50]})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


@pytest.mark.parametrize(
    "values", [[1], [1, 1, 1, 1, 1]], ids=("observation", "classes")
)
def test_parameters(check: Check, values):
    check.has_entropy("id", 0.0)
    df = pl.DataFrame({"id": values})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_entropy("id", 1.0, pct=0.5)
