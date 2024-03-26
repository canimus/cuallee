import daft
from cuallee import Check


def test_positive(check: Check):
    check.is_complete("id")
    df = daft.from_pydict({"id": [10, 20], "id2": [300, 500]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.is_complete("id")
    df = daft.from_pydict({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


def test_coverage(check: Check):
    check.is_complete("id", 0.5)
    df = daft.from_pydict({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()
    assert result.select(daft.col("pass_rate").max() == 0.5).to_pandas().pass_rate.all()
