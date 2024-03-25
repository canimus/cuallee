import daft
import pytest

from cuallee import Check

def test_positive(check: Check):
    check.are_unique(("id", "id2"))
    df = daft.from_pydict({"id": [10, 20], "id2": [300, 500]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.are_unique(("id", "id2"))
    df = daft.from_pydict({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


@pytest.mark.parametrize(
    "rule_column", [tuple(["id", "id2"]), list(["id", "id2"])], ids=("tuple", "list")
)
def test_parameters(check: Check, rule_column):
    check.are_unique(rule_column)
    df = daft.from_pydict({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


def test_coverage(check: Check):
    check.are_unique(("id", "id2"), 0.75)
    df = daft.from_pydict({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()
    assert result.select(daft.col("pass_rate").max() == 0.75).to_pandas().pass_rate.all()