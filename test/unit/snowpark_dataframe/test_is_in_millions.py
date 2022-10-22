import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_is_value_in_millions(snowpark, configurations):
    df = snowpark.range(10).withColumn("value", F.col("id") + 1e6)
    check = Check(CheckLevel.WARNING, "check_is_in_millions")
    check.is_in_millions("VALUE")
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"
