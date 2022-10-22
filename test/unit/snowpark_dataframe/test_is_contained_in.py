import pytest
import snowflake.snowpark.functions as F  # type: ignore

from datetime import datetime, date
from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_string_is_contained_in(snowpark, configurations):
    df = snowpark.createDataFrame(
        [[1, "blue"], [2, "green"], [3, "grey"]], ["id", "desc"]
    )
    check = Check(CheckLevel.WARNING, "is_contained_in_string_test")
    check.is_contained_in("DESC", ("blue", "red"))
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 2


def test_list_is_contained_in(snowpark, configurations):
    df = snowpark.createDataFrame(
        [[1, "blue"], [2, "green"], [3, "grey"]], ["id", "desc"]
    )
    check = Check(CheckLevel.WARNING, "is_contained_in_string_test")
    check.is_contained_in("DESC", ["blue", "red"])
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 2


def test_value_error_is_contained_in(
    snowpark, configurations
):  # TODO: type check is not executed as it should
    df = snowpark.createDataFrame([[1, 10], [2, 15], [3, 17]], ["id", "value"])
    check = Check(CheckLevel.WARNING, "is_contained_in_value_error")
    with pytest.raises(ValueError, match="Data types in rule values are inconsistent"):
        check.is_contained_in("VALUE", (10, "17"))


def test_number_is_contained_in(snowpark, configurations):
    df = snowpark.createDataFrame([[1, 10], [2, 15], [3, 17]], ["id", "value"])
    check = Check(CheckLevel.WARNING, "is_contained_in_number_test")
    check.is_contained_in("VALUE", (10, 13, 17))
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 1


def test_float_number_is_contained_in(snowpark, configurations):
    df = snowpark.createDataFrame([[1, 10], [2, 15], [3, 17]], ["id", "value"])
    check = Check(CheckLevel.WARNING, "is_contained_in_float_number_test")
    check.is_contained_in("VALUE", (10.5, 15.5, 17.0))
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 2


def test_date_is_contained_in(snowpark, configurations):
    df = snowpark.createDataFrame(
        [[1, date(2022, 10, 1)], [2, date(2022, 10, 2)], [3, date(2022, 10, 3)]],
        ["id", "date"],
    )
    check = Check(CheckLevel.WARNING, "is_contained_in_date_test")
    check.is_contained_in("DATE", (date(2022, 10, 1), date(2022, 11, 1)))
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 2


def test_timestamp_is_contained_in(snowpark, configurations):
    df = snowpark.createDataFrame(
        [
            [1, datetime(2022, 10, 1, 10, 0, 0)],
            [2, datetime(2022, 10, 1, 11, 0, 0)],
            [3, datetime(2022, 10, 1, 12, 0, 0)],
        ],
        ["id", "timestamp"],
    )
    check = Check(CheckLevel.WARNING, "is_contained_in_timestamp_test")
    check.is_contained_in(
        "TIMESTAMP", (datetime(2022, 10, 1, 9, 0, 0), datetime(2022, 10, 1, 10, 0, 0))
    )
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "FAIL"
    assert rs.first().VIOLATIONS == 2
