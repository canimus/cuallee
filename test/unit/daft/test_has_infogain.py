import daft
import numpy as np

from cuallee import Check


def test_positive(check: Check):
    check.has_infogain("id")
    df = daft.from_pydict({"id": np.arange(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.has_infogain("id")
    df = daft.from_pydict({"id": np.ones(10)})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()
