from cuallee import Control
import pandas as pd


def test_has_completeness():
    assert hasattr(Control, "completeness")


def test_has_information():
    assert hasattr(Control, "information")


def test_completeness_result():
    df = pd.DataFrame({"A": [1, 2, 3, 4, 5]})
    assert Control.completeness(df).status.eq("PASS").all()


def test_information_result():
    df = pd.DataFrame({"A": [1, 2, 3, 4, 5]})
    assert Control.information(df).status.eq("PASS").all()


def test_no_information_result():
    df = pd.DataFrame({"A": [1, 1, 1, 1, 1]})
    assert Control.information(df).status.eq("FAIL").any()


def test_fillness(spark):
    df = spark.range(10)
    assert Control.percentage_fill(df) == 1


def test_emptyness(spark):
    df = spark.range(10)
    assert Control.percentage_empty(df) == 0
