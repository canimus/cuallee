import pytest
import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame  # type: ignore
from cuallee import Check, CheckLevel


def test_positive(snowpark):
    df = snowpark.range(10).withColumn("desc", F.col("id").cast("string"))
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_complete(("ID", "DESC"))
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_negative(snowpark):
    df = snowpark.createDataFrame([[0, 'zero'], [1, None], [2, 'deux'], [3, 'trois']], ['id', 'desc'])
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_complete(("ID", "DESC"))
    rs = check.validate(df)
    assert rs.first().STATUS == "FAIL"


@pytest.mark.parametrize('rule_column', [tuple(['ID', 'DESC']), list(['ID', 'DESC'])], ids=('tuple', 'list'))
def test_parameters(snowpark, rule_column):
    df = snowpark.range(10).withColumn("desc", F.col("id").cast("string"))
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_complete(rule_column)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"


def test_coverage(snowpark):
    df = snowpark.createDataFrame([[0, 'zero'], [1, None], [2, 'deux'], [3, 'trois']], ['id', 'desc'])
    check = Check(CheckLevel.WARNING, "pytest")
    check.are_complete(("ID", "DESC"), 0.7)
    rs = check.validate(df)
    assert rs.first().STATUS == "PASS"
    assert rs.first().PASS_RATE == 0.7
