import polars as pl
from cuallee import Check, CustomComputeException
import pytest


def test_positive(check: Check):
    check.is_custom("id", lambda x: x.with_columns(test=pl.col("id") >= 0))
    df = pl.DataFrame({"id": range(10)})
    rs = check.validate(df).row(0, named=True)
    assert rs["status"] == "PASS"
    assert rs["violations"] == 0
    assert rs["pass_threshold"] == 1.0


def test_negative(check: Check):
    check.is_custom("id", lambda x: x.with_columns(test=pl.col("id") >= 5))
    df = pl.DataFrame({"id": range(10)})
    rs = check.validate(df).row(0, named=True)
    assert rs["status"] == "FAIL"
    assert rs["violations"] == 5
    assert rs["pass_threshold"] == 1.0


def test_parameters(check: Check):
    check.is_custom("id", "wrong value")
    df = pl.DataFrame({"id": range(10)})
    with pytest.raises(
        CustomComputeException,
        match="Please provide a Callable/Function for validation",
    ):
        check.validate(df)


def test_coverage(check: Check):
    check.is_custom("id", lambda x: x.with_columns(test=pl.col("id") >= 5), 0.4)
    df = pl.DataFrame({"id": range(10)})
    rs = check.validate(df).row(0, named=True)
    assert rs["status"] == "PASS"
    assert rs["pass_threshold"] == 0.4


def test_series(check: Check):
    check.is_custom("id", lambda x: x["id"] >= 5)
    df = pl.DataFrame({"id": range(10)})
    rs = check.validate(df).row(0, named=True)
    assert rs["status"] == "FAIL"
    assert rs["violations"] == 5


def test_null(check: Check):
    check.is_custom("id", lambda x: x.select(pl.col("id") >= 5))
    df = pl.DataFrame({"id": [10, None]})
    rs = check.validate(df).row(0, named=True)
    assert rs["status"] == "FAIL"
    assert rs["violations"] == 1


def test_numeric(check: Check):
    check.is_custom("id", lambda x: x.select("id"))
    df = pl.DataFrame({"id": [0, 1, 2]})
    rs = check.validate(df).row(0, named=True)
    assert rs["status"] == "FAIL"
    assert rs["violations"] == 1


def test_multiple_columns(check: Check):
    check.is_custom(("id", "id2"), lambda x: x.select(pl.col("id") < pl.col("id2")))
    df = pl.DataFrame({"id": [10, 20, 30], "id2": [300, 500, 5]})
    rs = check.validate(df).row(0, named=True)
    assert rs["status"] == "FAIL"
    assert rs["violations"] == 1


@pytest.mark.parametrize(
    "fn, message",
    [
        (lambda x: x.select(), "at least one column"),
        (lambda x: pl.col("id") >= 5, "Polars DataFrame or Series"),
        (lambda x: x.lazy(), "Polars DataFrame or Series"),
        (lambda x: None, "Polars DataFrame or Series"),
    ],
    ids=("empty", "expr", "lazyframe", "none"),
)
def test_return_type(check: Check, fn, message):
    check.is_custom("id", fn)
    df = pl.DataFrame({"id": range(10)})
    with pytest.raises(CustomComputeException, match=message):
        check.validate(df)
