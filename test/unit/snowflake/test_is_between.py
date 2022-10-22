import snowflake.snowpark.functions as F  # type: ignore

from datetime import datetime, date
from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_between_numbers(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "check_between_numbers")
    check.is_between("ID", (0, 10))
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"


def test_between_numbers_list(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "check_between_numbers_passed_as_list")
    check.is_between("ID", [0, 10])
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"


def test_between_numbers_with_pct(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "check_between_numbers_with_pct")
    check.is_between("ID", (0, 5), 0.5)
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"


def test_between_float_numbers(snowpark, configurations):
    df = snowpark.range(10).withColumn("number", F.col("id") + 1)
    check = Check(CheckLevel.WARNING, "check_between_float_numbers")
    check.is_between("NUMBER", (0.5, 10.5))
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"


def test_between_dates(snowpark, configurations):
    df = snowpark.range(10).withColumn("date", F.date_from_parts(2022, 10, F.col("id")))
    check = Check(CheckLevel.WARNING, "check_between_dates")
    check.is_between("DATE", (date(2022, 9, 1), date(2022, 11, 1)))
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"


def test_between_timestamps(snowpark, configurations):
    df = snowpark.range(10).withColumn(
        "timestamp", F.timestamp_from_parts(2022, 10, 22, F.col("id") + 5, 0, 0)
    )
    check = Check(CheckLevel.WARNING, "check_between_timestamps")
    check.is_between(
        "TIMESTAMP", (datetime(2022, 10, 22, 0, 0, 0), datetime(2022, 10, 22, 22, 0, 0))
    )
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"
