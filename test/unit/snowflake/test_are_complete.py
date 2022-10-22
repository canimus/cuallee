import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_are_complete(snowpark, configurations):
    df = snowpark.range(10).withColumn("desc", F.col("id").cast("string"))
    check = Check(CheckLevel.WARNING, "check_are_complete")
    check.are_complete(("ID", "DESC"))
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"


def test_are_complete_list_col(snowpark, configurations):
    df = snowpark.range(10).withColumn("desc", F.col("id").cast("string"))
    check = Check(CheckLevel.WARNING, "check_are_complete_with_list")
    check.are_complete(["ID", "DESC"])
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"
