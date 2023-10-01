import polars as pl
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.has_max_by("id", "id2", 20)
    df = pl.DataFrame({"id": [10, 20], "id2": [300, 500]})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.has_max_by("id", "id2", 10)
    df = pl.DataFrame({"id": [10, 20], "id2": [300, 500]})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


@pytest.mark.parametrize(
    "answer, columns",
    [(20, [10, 20]), ("herminio", ["antoine", "herminio"])],
    ids=("numeric", "string"),
)
def test_values(check: Check, answer, columns):
    check.has_max_by("id", "id2", answer)
    df = pl.DataFrame({"id": columns, "id2": [300, 500]})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_max_by("id", "id2", 20, 100)
