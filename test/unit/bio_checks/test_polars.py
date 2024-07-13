import pytest
import polars as pl

def test_is_dna(check):
    df = pl.DataFrame({"sequence" : ["ATGCCCTTTGGGTAA", "ATGCCCTTTGGGTAG", "ATGCCCTTTGGGTGA"]})
    check.bio.is_dna("sequence")
    rs = check.validate(df)
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())

def test_is_not_dna(check):
    df = pl.DataFrame({"sequence" : ["XXX", "YYY", "ZZZ"]})
    check.bio.is_dna("sequence")
    rs = check.validate(df)
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())

def test_is_cds(check):
    df = pl.DataFrame({"sequence" : ["ATGCCCTTTGGGTAA", "ATGCCCTTTGGGTAG", "ATGCCCTTTGGGTGA"]})
    check.bio.is_cds("sequence")
    rs = check.validate(df)
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_is_not_cds(check):
    df = pl.DataFrame({"sequence" : ["ATGCCCTTTGGGTCC", "ATGCCCTTTGGGCCC", "ATGCCCTTTGGGTTT"]})
    check.bio.is_cds("sequence")
    rs = check.validate(df)
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())

def test_is_protein(check):
    df = pl.DataFrame({"sequence" : ["ARND", "PSTW", "GHIL"]})
    check.bio.is_protein("sequence")
    rs = check.validate(df)
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())

def test_is_not_protein(check):
    df = pl.DataFrame({"sequence" : ["XXX", "OO1", "UU2"]})
    check.bio.is_protein("sequence")
    rs = check.validate(df)
    result = check.validate(df).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())