import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_are_unique(snowpark, configurations):
    df = snowpark.range(10).withColumn("id2", F.col("id") + 10)
    check = Check(CheckLevel.WARNING, "check_are_unique_set_of_values")
    check.are_unique(("ID", "ID2"))
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"


def test_are_unique_list_col(snowpark, configurations):
    df = snowpark.range(10).withColumn("id2", F.col("id") + 10)
    check = Check(CheckLevel.WARNING, "check_are_unique_set_of_values_with_list")
    check.are_unique(["ID", "ID2"])
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"