import pytest
import snowflake.snowpark.functions as F  # type: ignore

from snowflake.snowpark import DataFrame, Row  # type: ignore
from cuallee import Check, CheckLevel


def test_predicate_on_sql(snowpark, configurations):
    df = snowpark.range(10)
    check = Check(CheckLevel.WARNING, "check_predicate_on_sql")
    check.satisfies("ID", "((id BETWEEN 0 and 9) AND (id >= 0) AND (id <= 10))")
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"


def test_predicate_on_multi_column(snowpark, configurations):
    df = snowpark.range(10).withColumn("id2", F.col("id") * 100)
    check = Check(CheckLevel.WARNING, "check_predicate_on_multi_column")
    check.satisfies(["ID", "ID2"], "(id * id2) > 10", 0.9)
    check.config = configurations
    rs = check.validate(df)
    assert isinstance(rs, DataFrame)
    assert rs.first().STATUS == "PASS"


def test_predicate_on_unknown_columns(snowpark, configurations):
    df = snowpark.range(10).withColumn("id2", F.col("id") * 100)
    check = Check(CheckLevel.WARNING, "check_predicate_on_unknown_columns")
    check.satisfies(["ID", "ID3"], "(id * id2) > 10", 0.9)
    check.config = configurations
    with pytest.raises(AssertionError, match="not present in dataframe"):
        check.validate(df)
