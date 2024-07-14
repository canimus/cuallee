import pytest


def test_is_dna(check, spark):
    df = spark.createDataFrame(
        [("ATGCCCTTTGGGTAA",), ("ATGCCCTTTGGGTAG",), ("ATGCCCTTTGGGTGA",)],
        schema="sequence string",
    )
    check.bio.is_dna("sequence")
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_is_not_dna(check, spark):
    df = spark.createDataFrame([("XXX",), ("YYY",), ("ZZZ",)], schema="sequence string")
    check.bio.is_dna("sequence")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 3
    assert rs.first().pass_threshold == 1.0


def test_is_cds(check, spark):
    df = spark.createDataFrame(
        [("ATGCCCTTTGGGTAA",), ("ATGCCCTTTGGGTAG",), ("ATGCCCTTTGGGTGA",)],
        schema="sequence string",
    )
    check.bio.is_cds("sequence")
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_is_not_cds(check, spark):
    df = spark.createDataFrame(
        [("ATGCCCTTTGGGTCC",), ("ATGCCCTTTGGGCCC",), ("ATGCCCTTTGGGTTT",)],
        schema="sequence string",
    )
    check.bio.is_cds("sequence")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 3
    assert rs.first().pass_threshold == 1.0


def test_is_protein(check, spark):
    df = spark.createDataFrame(
        [("ARND",), ("PSTW",), ("GHIL",)], schema="sequence string"
    )
    check.bio.is_protein("sequence")
    rs = check.validate(df)
    assert rs.first().status == "PASS"
    assert rs.first().violations == 0
    assert rs.first().pass_threshold == 1.0


def test_is_not_protein(check, spark):
    df = spark.createDataFrame([("XXX",), ("OO1",), ("UU2",)], schema="sequence string")
    check.bio.is_protein("sequence")
    rs = check.validate(df)
    assert rs.first().status == "FAIL"
    assert rs.first().violations == 3
    assert rs.first().pass_threshold == 1.0
