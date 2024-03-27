import daft
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.is_unique("id")
    df = daft.from_pydict({"id": [10, 20, 30]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.is_unique("id")
    df = daft.from_pydict({"id": [10, 20, 30, 10]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


@pytest.mark.parametrize(
    "values",
    [[0, 1, 2], [0.1, 0.2, 0.0003], [1e2, 1e3, 1e4]],
    ids=("int", "float", "scientific"),
)
def test_parameters(check: Check, values):
    check.is_unique("id")
    df = daft.from_pydict({"id": values})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_coverage(check: Check):
    check.is_unique("id", pct=3 / 4)
    df = daft.from_pydict({"id": [10, 20, 30, 10]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()
    assert (
        result.select(daft.col("pass_rate").max() == 0.75).to_pandas().pass_rate.all()
    )
