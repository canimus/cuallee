import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_is_value_negative(snowpark, configurations):
    df = snowpark.range(10).withColumn("value", F.col("id") - 10)
    check = Check(CheckLevel.WARNING, "check_is_negative")
    check.is_negative("VALUE")
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"
