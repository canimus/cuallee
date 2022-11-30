import pytest
import snowflake.snowpark.functions as F  # type: ignore

from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10).withColumn("VALUE", 1000 - F.col("id"))
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_less_or_equal_than("VALUE", 1000)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 0
    assert rs.first().PASS_THRESHOLD == 1.0


def test_negative(snowpark):
    df = snowpark.createDataFrame(
        [[1060], [1], [1073], [1000], [10]], ["VALUE"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_less_or_equal_than("VALUE", 1000)
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 2
    assert rs.first().PASS_THRESHOLD == 1.0
    assert rs.first().PASS_RATE == 3/5


@pytest.mark.parametrize(
    "rule_value", [int(1000), float(1000)], ids=("int", "float")
)
def test_parameters(snowpark, rule_value):
    df = snowpark.range(10).withColumn("VALUE", 1000 - F.col("id"))
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_less_or_equal_than("VALUE", rule_value)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage(snowpark):
    df = snowpark.createDataFrame(
        [[1060], [1], [1073], [1000], [10]], ["VALUE"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_less_or_equal_than("VALUE", 1000, 0.6)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 2
    assert rs.first().PASS_THRESHOLD == 0.6
    assert rs.first().PASS_RATE == 3/5