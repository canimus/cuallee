import pandas as pd

from cuallee import Control


def test_has_completeness():
    assert hasattr(Control, "completeness")


def test_has_information():
    assert hasattr(Control, "information")


def test_has_information():
    assert hasattr(Control, "intelligence")


def test_completeness_result():
    df = pd.DataFrame({"A": [1, 2, 3, 4, 5]})
    assert Control.completeness(df).status.eq("PASS").all()


def test_fillness(spark):
    df = spark.range(10)
    assert Control.percentage_fill(df) == 1


def test_emptyness(spark):
    df = spark.range(10)
    assert Control.percentage_empty(df) == 0


def test_intelligence_result(spark):
    df = spark.createDataFrame([("0",), ("1",), ("2",)], schema="id string")
    assert Control.intelligence(df) == ["id"]
