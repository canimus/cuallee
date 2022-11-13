import pandas as pd
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.has_min_by("id", "id2", 10)
    df = pd.DataFrame({"id": [10, 20], "id2": [300, 500]})
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.has_min_by("id", "id2", 20)
    df = pd.DataFrame({"id": [10, 20], "id2": [300, 500]})
    assert check.validate(df).status.str.match("FAIL").all()


@pytest.mark.parametrize(
    "answer, columns",
    [(10, [10, 20]), ("antoine", ["antoine", "herminio"])],
    ids=("numeric", "string"),
)
def test_values(check: Check, answer, columns):
    check.has_min_by("id", "id2", answer)
    df = pd.DataFrame({"id": columns, "id2": [300, 500]})
    assert check.validate(df).status.str.match("PASS").all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_min_by("id", "id2", 10, 0.75)
