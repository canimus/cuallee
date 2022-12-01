import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(5).withColumn(
        "ARRIVAL_DATE", F.date_from_parts(2022, 11, F.col("id") + 14)
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_on_weekday("ARRIVAL_DATE")
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VIOLATIONS == 0
    assert rs.first().PASS_THRESHOLD == 1.0
    

def test_is_on_weekend(snowpark, configurations):
    df = snowpark.range(10).withColumn(
        "date", F.date_from_parts(2022, 1, F.col("id") + 1)
    )
    check = Check(CheckLevel.WARNING, "check_is_on_weekend")
    check.is_on_weekend("DATE")
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 6, "Incorrect calulation of Weekend filters"
