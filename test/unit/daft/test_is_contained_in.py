import daft
import numpy as np

from cuallee import Check


def test_positive(check: Check):
    check.is_contained_in("id", [0, 1, 2, 3, 4, 5])
    df = daft.from_pydict({"id": np.arange(5)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.is_contained_in("id", [0, 1, 2, 3])
    df = daft.from_pydict({"id": np.arange(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


def test_coverage(check: Check):
    check.is_contained_in(
        "id",
        [
            0,
            1,
            2,
            3,
            4,
        ],
        0.50,
    )
    df = daft.from_pydict({"id": np.arange(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()
    col_pass_rate = daft.col("pass_rate")
    assert (
        result.agg(col_pass_rate.max())
        .select(col_pass_rate == 0.50)
        .to_pandas()
        .pass_rate.all()
    )
