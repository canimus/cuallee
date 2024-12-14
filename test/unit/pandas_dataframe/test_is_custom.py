import pytest

from cuallee import Check, CheckLevel, CustomComputeException
import pandas as pd


def test_positive(check: Check):
    df = pd.DataFrame({"id": range(10)})
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_custom("id", lambda x: x.assign(test=(x["id"] >= 0)))
    rs = check.validate(df)
    assert rs.iloc[0].status == "PASS"
    assert rs.iloc[0].violations == 0
    assert rs.iloc[0].pass_threshold == 1.0


def test_negative(check: Check):
    df = pd.DataFrame({"id": range(10)})
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_custom("id", lambda x: x.assign(test=(x["id"] >= 5)))
    rs = check.validate(df)
    assert rs.iloc[0].status == "FAIL"
    assert rs.iloc[0].violations == 5
    assert rs.iloc[0].pass_threshold == 1.0


def test_parameters(check: Check):
    df = pd.DataFrame({"id": range(10)})
    with pytest.raises(
        CustomComputeException,
        match="Please provide a Callable/Function for validation",
    ):
        check = Check(CheckLevel.WARNING, "pytest")
        check.is_custom("id", "wrong value")
        check.validate(df)


def test_coverage(check: Check):
    df = pd.DataFrame({"id": range(10)})
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_custom("id", lambda x: x.assign(test=(x["id"] >= 5)), 0.4)
    rs = check.validate(df)
    assert rs.iloc[0].status == "PASS"
    assert rs.iloc[0].pass_threshold == 0.4
