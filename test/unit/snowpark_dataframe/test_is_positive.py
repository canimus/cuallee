import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_is_value_positive(snowpark, configurations):
    df = snowpark.range(10).withColumn("value", F.col("id") + 1000)
    check = Check(CheckLevel.WARNING, "check_is_positive")
    check.is_positive("VALUE")
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"
