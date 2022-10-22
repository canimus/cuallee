import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_is_on_sunday(snowpark, configurations):
    df = snowpark.range(10).withColumn(
        "date", F.date_from_parts(2022, 1, F.col("id") + 1)
    )
    check = Check(CheckLevel.WARNING, "check_is_on_sunnday")
    check.is_on_sunday("DATE")
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 8, "Incorrect calulation of Sunday filters"
