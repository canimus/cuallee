import daft

from cuallee import Check


def test_positive(check: Check):
    check.is_in_millions("id")
    df = daft.from_pydict({"id": [1e6, 1e6 + 1]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.is_in_millions("id")
    df = daft.from_pydict({"id": [1, 2]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


def test_coverage(check: Check):
    check.is_in_millions("id", 0.5)
    df = daft.from_pydict({"id": [1.0, 1e6]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()

    col_pass_rate = daft.col("pass_rate")
    assert (
        result.agg(col_pass_rate.max())
        .select(col_pass_rate == 0.50)
        .to_pandas()
        .pass_rate.all()
    )
