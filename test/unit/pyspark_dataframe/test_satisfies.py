import pytest
import pyspark.sql.functions as F

#from pyspark.sql.utils import AnalysisException
from pyspark.errors.exceptions.captured import AnalysisException
from cuallee import Check, CheckLevel


def test_positive(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.satisfies("id", "((id BETWEEN 0 and 9) AND (id >= 0) AND (id <= 10))")
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_negative(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.satisfies("id", "((id BETWEEN 0 and 9) AND (id > 0) AND (id <= 10))")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"


@pytest.mark.parametrize(
    "columns, rule_value",
    [
        ["id", "((id BETWEEN 0 and 9) AND (id >= 0) AND (id <= 10))"],
        [tuple(["id", "id2"]), "((id * id2) > 10) OR ((id * id2) = 0)"],
        [list(["id", "id2"]), "((id * id2) > 10) OR ((id * id2) = 0)"],
    ],
    ids=("one_column", "two_columns_tuple", "two_columns_list"),
)
def test_parameters(spark, columns, rule_value):
    df = spark.range(10).withColumn("id2", (F.col("id") + 1) * 100)
    check = Check(CheckLevel.WARNING, "pytest")
    check.satisfies(columns, rule_value)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_coverage(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.satisfies("id", "((id BETWEEN 0 and 9) AND (id > 0) AND (id <= 10))", 0.9)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 1
    assert rs.first().pass_threshold == 0.9
    assert rs.first().pass_rate == 0.9


def test_col_name_error(spark):
    df = spark.range(10).withColumn("id2", F.col("id") * 100)
    check = Check(CheckLevel.WARNING, "check_predicate_on_unknown_columns")
    check.satisfies(["id", "id2"], "(id * id3) > 10", 0.9)
    with pytest.raises(AnalysisException, match=r"A column or function parameter with name `id3` cannot be resolved"):
        check.validate(df)


def test_unknown_columns(spark):
    df = spark.range(10).withColumn("id2", F.col("id") * 100)
    check = Check(CheckLevel.ERROR, "SatisfiesTest")
    check.satisfies(["id", "id3"], "(id * id2) > 10", 0.9)

    with pytest.raises(AssertionError, match=r".*id3.* not present in dataframe"):
        check.validate(df).first().status == "PASS"
