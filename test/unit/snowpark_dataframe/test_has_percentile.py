import pytest
import snowflake.snowpark.functions as F  # type: ignore

from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_percentile("ID", 4.5, 0.5)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_percentile("ID", 4.0, 0.5)
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"


@pytest.mark.parametrize("data_type, rule_value", [[[[0], [2], [4]], int(3)], [[[0], [2], [4]], float(3.0)], [[[float(0.0)], [float(2.0)], [float(4.0)]], int(3)], [[[float(0.0)], [float(2.0)], [float(4.0)]], float(3.0)]], ids=["value_int", "value_float", "data_int", "data_float"])
def test_parameters(snowpark, data_type, rule_value):
    df = snowpark.createDataFrame(
        data_type, ["TEST"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_percentile("TEST", rule_value, 0.5)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage():
    check = Check(CheckLevel.WARNING, "pytest")
    with pytest.raises(TypeError, match="positional arguments"):
        check.has_percentile("ID", 3.0, 0.5, 1000, 0.7)


@pytest.mark.parametrize(
    "percentage, precision, expected",
    [[0.5, 1000, "PASS"], [1.0, 1000, "FAIL"], [0.5, 100000000, "PASS"]], 
    ids=["percentage_pass", "percentage_fail", "precision"],
)
def test_settings(snowpark, percentage, precision, expected): 
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_percentile("ID", 4.5, percentage, precision)
    rs = check.validate(df)
    assert rs.first().STATUS == expected
