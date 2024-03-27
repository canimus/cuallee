import daft
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.has_max_by("id", "id2", 20)
    df = daft.from_pydict({"id": [10, 20], "id2": [300, 500]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.has_max_by("id", "id2", 10)
    df = daft.from_pydict({"id": [10, 20], "id2": [300, 500]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


@pytest.mark.parametrize(
    "answer, columns",
    [(20, [10, 20]), ("herminio", ["antoine", "herminio"])],
    ids=("numeric", "string"),
)
def test_values(check: Check, answer, columns):
    check.has_max_by("id", "id2", answer)
    df = daft.from_pydict({"id": columns, "id2": [300, 500]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_max_by("id", "id2", 20, 100)
