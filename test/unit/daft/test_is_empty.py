import daft

from cuallee import Check


def test_positive(check: Check):
    check.is_empty("id")
    df = daft.from_pydict({"id": [None, None], "id2": [None, None]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.is_empty("id")
    df = daft.from_pydict({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


def test_coverage(check: Check):
    check.is_empty("id", 0.5)
    df = daft.from_pydict({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()
    col_pass_rate = daft.col("pass_rate")
    assert (
        result.agg(col_pass_rate.max())
        .select(col_pass_rate == 0.50)
        .to_pandas()
        .pass_rate.all()
    )
