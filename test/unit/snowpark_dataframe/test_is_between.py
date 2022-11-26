import pytest
import snowflake.snowpark.functions as F  # type: ignore

from datetime import datetime, date
from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_between("ID", (0, 10))
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_between("ID", (0, 5))
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"


@pytest.mark.parametrize("data, rule_column, rule_value", [[F.col('ID'), "ID_2", tuple([0, 10])], [F.col('ID'), "ID_2", list([0, 10])], [F.col('ID').cast('float'), "ID_2", tuple([0, 10])], [F.col("id") + 1, "NUMBER", tuple([float(0.5), float(10.5)])], [F.date_from_parts(2022, 10, F.col("id")), "DATE", (date(2022, 9, 1), date(2022, 11, 1))], [F.timestamp_from_parts(2022, 10, 22, F.col("id") + 5, 0, 0), "TIMESTAMP", (datetime(2022, 10, 22, 0, 0, 0), datetime(2022, 10, 22, 22, 0, 0))]], ids=["tuple", "list", "data_as_float", "value_float", "date", "timestamp"])
def test_parameters(snowpark, data, rule_column, rule_value):
    df = snowpark.range(10).withColumn(rule_column, data)
    check = Check(CheckLevel.WARNING, "pytest") 
    check.is_between(rule_column, rule_value)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage(snowpark):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_between("ID", (0, 5), 0.5)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"