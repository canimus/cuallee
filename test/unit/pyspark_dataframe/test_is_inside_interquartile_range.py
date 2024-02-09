import pytest
import pyspark.sql.functions as F

from cuallee import Check, CheckLevel


import pandas as pd


def test_positive(spark):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_inside_interquartile_range("id", pct=0.50)
    rs = check.validate(df)
    assert rs.first().status == "PASS"


def test_negative(spark):  # TODO: check why results are different than snwopark
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_inside_interquartile_range("id")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 4
    assert rs.first().pass_threshold == 1.0
    assert rs.first().pass_rate >= 0.6


@pytest.mark.parametrize(
    "rule_value, value",
    [
        [list([0.25, 0.75]), "(0.25, 0.75)"],
        [tuple([0.25, 0.75]), "(0.25, 0.75)"],
        [list([0.1, 0.8]), "(0.1, 0.8)"],
    ],
    ids=("list", "tuple", "other_values"),
)
def test_parameters(spark, rule_value, value):
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_inside_interquartile_range("id", rule_value, pct=0.50)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().value == value


def test_coverage(spark):  # TODO: check why results are different than snwopark
    df = spark.range(10)
    check = Check(CheckLevel.WARNING, "pytest")
    check.is_inside_interquartile_range("id", pct=0.5)
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 4
    assert rs.first().pass_threshold == 0.5
    assert rs.first().pass_rate >= 0.6


# def test_tendency(spark):
#     df = spark.createDataFrame(
#         pd.DataFrame(
#             {
#                 "id": [
#                     12,
#                     13,
#                     15,
#                     18,
#                     19,
#                     22,
#                     88,
#                     89,
#                     90,
#                     91,
#                     92,
#                     93,
#                     95,
#                     98,
#                     99,
#                     101,
#                     101,
#                     103,
#                     105,
#                     106,
#                     107,
#                     108,
#                     109,
#                     200,
#                     201,
#                     201,
#                     203,
#                     204,
#                     215,
#                     216,
#                     217,
#                     222,
#                     223,
#                     224,
#                     225,
#                     227,
#                     229,
#                     230,
#                     232,
#                     245,
#                     246,
#                     250,
#                     258,
#                     270,
#                     271,
#                     271,
#                     272,
#                     273,
#                 ]
#             }
#         )
#     )
#     check = Check(CheckLevel.WARNING, "IQRTest")
#     check.is_inside_interquartile_range("id")
#     assert check.validate(df).first().violations == 23


# def test_iqr_pct(spark):
#     df = spark.createDataFrame(
#         pd.DataFrame(
#             {
#                 "id": [
#                     12,
#                     13,
#                     15,
#                     18,
#                     19,
#                     22,
#                     88,
#                     89,
#                     90,
#                     91,
#                     92,
#                     93,
#                     95,
#                     98,
#                     99,
#                     101,
#                     101,
#                     103,
#                     105,
#                     106,
#                     107,
#                     108,
#                     109,
#                     200,
#                     201,
#                     201,
#                     203,
#                     204,
#                     215,
#                     216,
#                     217,
#                     222,
#                     223,
#                     224,
#                     225,
#                     227,
#                     229,
#                     230,
#                     232,
#                     245,
#                     246,
#                     250,
#                     258,
#                     270,
#                     271,
#                     271,
#                     272,
#                     273,
#                 ]
#             }
#         )
#     )
#     check = Check(CheckLevel.WARNING, "IQRTest")
#     check.is_inside_interquartile_range("id", pct=0.5)
#     assert check.validate(df).first().status == "PASS"


# def test_integers(spark):
#     df = spark.range(10)
#     check = Check(CheckLevel.WARNING, "IQR_Test")
#     check.is_inside_interquartile_range("id", pct=0.4)
#     assert check.validate(df).first().status == "PASS"
