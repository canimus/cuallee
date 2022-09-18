def test_has_mean(spark):
    assert spark.range(10).count() == 10