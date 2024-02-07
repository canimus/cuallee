import pandas as pd
from cuallee import Check
import pytest


def test_negative(check: Check):
    check.not_contained_in("id", [0, 1, 2, 3, 4, 5])
    df = pd.DataFrame({"id": range(5)})
    assert check.validate(df).status.str.match("FAIL").all()


def test_positive(check: Check):
    check.not_contained_in("id", [0, 1, 2, 3])
    df = pd.DataFrame({"id": range(10,20)})
    assert check.validate(df).status.str.match("PASS").all()


def test_coverage(check: Check):
    check.not_contained_in(
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
    df = pd.DataFrame({"id": range(10)})
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.5
