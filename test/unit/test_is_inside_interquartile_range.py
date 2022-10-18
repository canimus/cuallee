from cuallee import Check, CheckLevel
import pandas as pd

def test_tendency(spark):
    df = spark.createDataFrame(pd.DataFrame({"id": [12, 13, 15, 18, 19, 22, 88, 89, 90, 91, 92, 93, 95, 98, 99, 101, 101, 103, 105, 106, 107, 108, 109, 200, 201, 201, 203, 204, 215, 216, 217, 222, 223, 224, 225, 227, 229, 230, 232, 245, 246, 250, 258, 270, 271, 271, 272, 273]}))
    check = Check(CheckLevel.WARNING, "IQRTest")
    check.is_inside_interquartile_range("id")
    assert check.validate(df).first().violations == 23


def test_iqr_pct(spark):
    df = spark.createDataFrame(pd.DataFrame({"id": [12, 13, 15, 18, 19, 22, 88, 89, 90, 91, 92, 93, 95, 98, 99, 101, 101, 103, 105, 106, 107, 108, 109, 200, 201, 201, 203, 204, 215, 216, 217, 222, 223, 224, 225, 227, 229, 230, 232, 245, 246, 250, 258, 270, 271, 271, 272, 273]}))
    check = Check(CheckLevel.WARNING, "IQRTest")
    check.is_inside_interquartile_range("id", pct=0.5)
    assert check.validate(df).first().status == "PASS"