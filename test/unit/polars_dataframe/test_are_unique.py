from cuallee import Check
import polars as pl
import pytest


def test_positive(check: Check):
    check.are_unique(("id", "id2"))
    df = pl.DataFrame({"id": [10, 20], "id2": [300, 500]})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_negative(check: Check):
    check.are_unique(("id", "id2"))
    df = pl.DataFrame({"id": [10, 10], "id2": [20, 20]})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


@pytest.mark.parametrize(
    "rule_column", [tuple(["id", "id2"]), list(["id", "id2"])], ids=("tuple", "list")
)
def test_parameters(check: Check, rule_column):
    check.are_unique(rule_column)
    df = pl.DataFrame({"id": [10, 10], "id2": [20, 20]})
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


def test_coverage(check: Check):
    check.are_unique(("id", "id2"), 0.75)
    df = pl.DataFrame({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df)
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())
