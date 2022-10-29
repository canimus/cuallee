import pandas as pd
from cuallee import Check
import pytest


def test_positive(check: Check):
    check.are_complete(("id", "id2"))
    df = pd.DataFrame({"id": [10, 20], "id2": [300, 500]})
    assert check.validate(df).status.str.match("PASS").all()


def test_negative(check: Check):
    check.are_complete(("id", "id2"))
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    assert check.validate(df).status.str.match("FAIL").all()

@pytest.mark.parametrize("rule_column", [tuple(["id", "id2"]), list(["id", "id2"])], ids=("tuple", "list"))
def test_parameters(check: Check, rule_column):
    check.are_complete(rule_column)
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df)
    assert result.status.str.match("FAIL").all()

def test_coverage(check: Check):
    check.are_complete(("id", "id2"), 0.75)
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
