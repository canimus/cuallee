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
        "c50361d748fe3ca294ed85751df113f4f7fbdb6faccb48ec531d865b71170268"
    )
    assert len(c._rule) == 2

    c.validate(df)
    assert len(c._rule) == 2
    
    c.delete_rule_by_key(
        "33403fdc43f5c7665952ad9885063ab09e1c233716eaa21a4da49817eec8fb70"
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
        "c50361d748fe3ca294ed85751df113f4f7fbdb6faccb48ec531d865b71170268"
    )
    assert len(c._rule) == 2
    
    c.validate(df)
    assert len(c._rule) == 2
    
    c.delete_rule_by_key(
        [
            "33403fdc43f5c7665952ad9885063ab09e1c233716eaa21a4da49817eec8fb70",
            "b342433c79ccb3ea8486bfb8b237b27bf74b81b37778cdb812ffb250111330b5",
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
    
