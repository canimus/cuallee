import daft
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.has_entropy("id", 1.0)
    df = daft.from_pydict({"id": [1, 1, 1, 0, 0, 0]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()



def test_negative(check: Check):
    check.has_entropy("id", 1.0)
    df = daft.from_pydict({"id": [10, 10, 10, 10, 50]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


@pytest.mark.parametrize(
    "values", [[1], [1, 1, 1, 1, 1]], ids=("observation", "classes")
)
def test_parameters(check: Check, values):
    check.has_entropy("id", 0.0)
    df = daft.from_pydict({"id": values})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_entropy("id", 1.0, pct=0.5)
