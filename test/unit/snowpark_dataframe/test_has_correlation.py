import pytest
import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame, Row  # type: ignore
from cuallee import Check, CheckLevel

@pytest.mark.parametrize('column', [F.col('ID'), F.col('ID').cast('double').alias('ID')], ids=['int', 'float'])
def test_positive(snowpark, column):
    df = snowpark.range(10).select(column).withColumn("ID2", F.col("ID") * 10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_correlation("ID", "ID2", 1.0)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().VALUE == "1.0"


def test_negative(snowpark):
    df = snowpark.createDataFrame(
        [
            Row(
                id1,
                id2,
            )
            for id1, id2 in enumerate([F.randint(0, 100) for f in range(10)])
        ],
        schema=["ID", "ID2"],
    )
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_correlation("ID", "ID2", 1.0)
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"


@pytest.mark.parametrize('rule_value', [int(1), float(1.0)], ids=['int', 'float'])
def test_parameters(snowpark, rule_value):
    df = snowpark.range(10).withColumn("id2", F.col("id") * 10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_correlation("ID", "ID2", rule_value)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage(snowpark):
    df = snowpark.createDataFrame([[0, 0], [1, 1], [2, 2], [3, 3], [4, 4], [5, 9], [6, 14], [7, 87], [8, 53], [9, 543]], ['ID', 'ID2'])
    check = Check(CheckLevel.WARNING, "pytest")
    check.has_correlation("ID", "ID2", 1.0, 0.5)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"