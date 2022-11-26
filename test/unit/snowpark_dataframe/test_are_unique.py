import pytest
import snowflake.snowpark.functions as F  # type: ignore

from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10).withColumn("id2", F.col("id") + 10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_unique(("ID", "ID2"))
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.createDataFrame(
        [[0, "zero"], [2, "deux"], [2, "deux"], [3, "trois"]], ["id", "desc"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_unique(("ID", "DESC"))
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"


@pytest.mark.parametrize(
    "rule_column", [tuple(["ID", "ID2"]), list(["ID", "ID2"])], ids=("tuple", "list")
)
def test_parameters(snowpark, rule_column):
    df = snowpark.range(10).withColumn("id2", F.col("id") + 10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_unique(rule_column)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage(snowpark):
    df = snowpark.createDataFrame(
        [[0, "zero"], [2, "deux"], [2, "deux"], [3, "trois"]], ["id", "desc"]
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_unique(("ID", "DESC"), 0.7)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().PASS_THRESHOLD == 0.7
    assert rs.first().PASS_RATE == 3 / 4
