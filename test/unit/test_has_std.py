from cuallee import Check, CheckLevel
import numpy as np


def test_sigmas(spark):
    df = spark.range(10)
    check = Check(CheckLevel.ERROR, "StdDevTest")
    check.has_std("id", np.arange(10).std())
    assert check.validate(spark, df).first().status == "PASS"
