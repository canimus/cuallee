import pyspark.sql.functions as F

from cuallee import Check, CheckLevel


def test_add_rule():
    c = Check(CheckLevel.WARNING, "test_add_rule").is_complete("id")
    assert len(c._rule) == 1
    c.add_rule("is_complete", "desc")
    assert len(c._rule) == 2


def test_delete_rule_by_key(spark):
    df = spark.range(10).alias("id")
    c = (
        Check(CheckLevel.WARNING, "test_delete_rule_by_key")
        .is_complete("id")
        .is_unique("id")
        .is_greater_or_equal_than("id", 2)
    )
    assert len(c._rule) == 3

    c.delete_rule_by_key(
        "5F0384C5F6A020BD1B2CACB1F11CD94B7B19BC1551041B20E767CAB44E559895"
    )
    assert len(c._rule) == 2

    c.validate(df)
    assert len(c._rule) == 2

    c.delete_rule_by_key(
        "EB58BDA44A0A4A4A2B4510C2F6013DB7D59F450C4FC8A3D628A370D4D8872DF7"
    )
    assert len(c._rule) == 1


def test_delete_rule_by_list_of_keys(spark):
    df = spark.range(10).alias("id")
    c = (
        Check(CheckLevel.WARNING, "test_delete_rule_by_list_of_keys")
        .is_complete("id")
        .is_unique("id")
        .is_greater_or_equal_than("id", 2)
    )
    assert len(c._rule) == 3

    c.delete_rule_by_key(
        "A4846EC1BEF648CE1D190492031AA5999A61EAEF0C13B250439569539FD3E83C"
    )
    assert len(c._rule) == 2

    c.validate(df)
    assert len(c._rule) == 2

    c.delete_rule_by_key(
        [
            "EB58BDA44A0A4A4A2B4510C2F6013DB7D59F450C4FC8A3D628A370D4D8872DF7",
            "5F0384C5F6A020BD1B2CACB1F11CD94B7B19BC1551041B20E767CAB44E559895",
        ]
    )
    assert len(c._rule) == 0


def test_delete_rule_by_method(spark):
    df = spark.range(10).alias("id").withColumn("desc", F.col("id").cast("string"))
    c = (
        Check(CheckLevel.WARNING, "test_delete_rule_by_method")
        .is_complete("id")
        .is_complete("desc")
        .is_greater_or_equal_than("id", 2)
    )
    c.validate(df)
    assert len(c._rule) == 3

    c.delete_rule_by_attribute("method", ["is_complete"])
    assert len(c._rule) == 1


def test_delete_rule_by_column(spark):
    df = spark.range(10).alias("id").withColumn("desc", F.col("id").cast("string"))
    c = (
        Check(CheckLevel.WARNING, "test_delete_rule_by_column")
        .is_complete("id")
        .is_complete("desc")
        .is_greater_or_equal_than("id", 2)
    )
    c.validate(df)
    assert len(c._rule) == 3

    c.delete_rule_by_attribute("column", ["id"])
    assert len(c._rule) == 1


def test_delete_rule_by_coverage(spark):
    df = spark.range(10).alias("id").withColumn("desc", F.col("id").cast("string"))
    c = (
        Check(CheckLevel.WARNING, "test_delete_rule_by_coverage")
        .is_complete("id")
        .is_complete("desc")
        .is_greater_or_equal_than("id", 2)
    )
    c.validate(df)
    assert len(c._rule) == 3

    c.delete_rule_by_attribute("coverage", 1.0)
    assert len(c._rule) == 0
