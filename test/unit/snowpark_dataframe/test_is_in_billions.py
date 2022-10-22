import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_is_value_in_billions(snowpark, configurations):
    df = snowpark.range(10).withColumn("value", F.col("id") + 1e9)
    check = Check(CheckLevel.WARNING, "check_is_in_billions")
    check.is_in_billions("VALUE")
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"
