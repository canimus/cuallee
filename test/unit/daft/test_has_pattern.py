import daft
import pytest

from cuallee import Check


def test_positive(check: Check):
    check.has_pattern("id", r"^H.*")
    df = daft.from_pydict({"id": ["Herminio", "Hola", "Heroe"]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_is_legit(check: Check):
    check.is_legit("id")
    df = daft.from_pydict({"id": ["Herminio", "Hola", "Heroe"]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_negative(check: Check):
    check.has_pattern("id", r"^H.*")
    df = daft.from_pydict({"id": ["Herminio", "Hola", "Villain"]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


def test_is_not_legit(check: Check):
    check.is_legit("id")
    df = daft.from_pydict({"id": ["Herminio", "Hola", ""]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("FAIL")).to_pandas().status.all()


@pytest.mark.parametrize(
    "pattern", ["^H.*", "^H.*"], ids=("single_quote", "double_quote")
)
def test_values(check: Check, pattern):
    check.has_pattern("id", pattern)
    df = daft.from_pydict({"id": ["Herminio", "Hola", "Heroe"]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()


def test_coverage(check: Check):
    check.has_pattern("id", r"^H.*", 0.75)
    df = daft.from_pydict({"id": ["Herminio", "Hola", "Villain", "Heroe"]})
    result = check.validate(df)
    assert result.select(daft.col("status").str.match("PASS")).to_pandas().status.all()
    assert result.select(daft.col("pass_rate").max() == 0.75).to_pandas().pass_rate.all()
