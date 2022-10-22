import snowflake.snowpark.functions as F  # type: ignore

from datetime import datetime
from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_is_on_schedule(snowpark, configurations):
    df = snowpark.range(10).withColumn(
        "timestamp", F.timestamp_from_parts(2022, 1, 1, F.col("id") + 7, 0, 0)
    )
    check = Check(CheckLevel.WARNING, "check_is_on_schedule")
    check.is_on_schedule("TIMESTAMP", (9, 18))
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 2
