from cuallee import Check
import pandas as pd


def test_numeric_columns(check: Check):
    check.are_complete(("id", "id2"))
    df = pd.DataFrame({"id": [10, 20], "id2": [300, 500]})
    assert check.validate(df).status.str.match("PASS").all()


def test_empty_columns(check: Check):
    check.are_complete(("id", "id2"))
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    assert check.validate(df).status.str.match("FAIL").all()


def test_coverage(check: Check):
    check.are_complete(("id", "id2"), 0.75)
    df = pd.DataFrame({"id": [10, None], "id2": [300, 500]})
    result = check.validate(df)
    assert result.status.str.match("PASS").all()
    assert result.pass_rate.max() == 0.75
