import pandas as pd
import numpy as np
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.has_pattern("id", r"^H.*")
    df = pd.DataFrame({"id": ["Herminio", "Hola", "Heroe"]})
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.has_pattern("id", r"^H.*")
    df = pd.DataFrame({"id": ["Herminio", "Hola", "Villain"]})
    assert check.validate(df).status.str.match("FAIL").all()


@pytest.mark.parametrize(
    "pattern", ["^H.*", "^H.*"], ids=("single_quote", "double_quote")
)
def test_values(check: Check, pattern):
    check.has_pattern("id", pattern)
    df = pd.DataFrame({"id": ["Herminio", "Hola", "Heroe"]})
    assert check.validate(df).status.str.match("PASS").all()


def test_coverage(check: Check):
    check.has_pattern("id", r"^H.*", 0.75)
    df = pd.DataFrame({"id": ["Herminio", "Hola", "Villain", "Heroe"]})
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.75
