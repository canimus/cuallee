import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_is_complete(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "check_is_complete")
    check.is_complete("ID")
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"


def test_name_with_space(snowpark, configurations):
    df = snowpark.range(10).withColumn("id 2", F.col("id"))
    check = Check(CheckLevel.WARNING, "check_name_with_space")
    check.is_complete('"id 2"')
    check.config = configurations
    assert check.validate(df).first().STATUS == "PASS"


def test_name_with_dot(snowpark, configurations):
    df = snowpark.range(10).withColumn("id.2", F.col("id"))
    check = Check(CheckLevel.WARNING, "check_name_with_space")
    check.is_complete('"id.2"')
    check.config = configurations
    assert check.validate(df).first().STATUS == "PASS"
