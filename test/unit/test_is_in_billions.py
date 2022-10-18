from cuallee import Check, CheckLevel

def test_natural_numbers(spark):
    df = spark.createDataFrame([(1e9, ), (1e9+1, )], schema="id float")
    check = Check(CheckLevel.WARNING, "Zero Failure")
    check.is_in_billions("id")
    assert check.validate(df).first().status == "PASS"

def test_thousands(spark):
    df = spark.createDataFrame([(1000.0, ), (1001.0, )], schema="id float")
    check = Check(CheckLevel.WARNING, "Zero Failure")
    check.is_in_billions("id")
    assert check.validate(df).first().status == "FAIL"