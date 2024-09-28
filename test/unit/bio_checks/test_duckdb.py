import pytest
import polars as pl
import duckdb


def test_is_dna(check, db: duckdb.DuckDBPyConnection):
    df = pl.DataFrame(
        {"sequence": ["ATGCCCTTTGGGTAA", "ATGCCCTTTGGGTAG", "ATGCCCTTTGGGTGA"]}
    )
    check.table_name = "df"
    db.register("df", df)
    check.bio.is_dna("sequence")
    assert check.validate(db).status.str.match("PASS").all()


def test_is_not_dna(check, db: duckdb.DuckDBPyConnection):
    df = pl.DataFrame({"sequence": ["XXX", "YYY", "ZZZ"]})
    check.table_name = "df"
    db.register("df", df)
    check.bio.is_dna("sequence")
    assert check.validate(db).status.str.match("FAIL").all()


def test_is_cds(check, db: duckdb.DuckDBPyConnection):
    df = pl.DataFrame(
        {"sequence": ["ATGCCCTTTGGGTAA", "ATGCCCTTTGGGTAG", "ATGCCCTTTGGGTGA"]}
    )
    check.table_name = "df"
    db.register("df", df)
    check.bio.is_cds("sequence")
    assert check.validate(db).status.str.match("PASS").all()


def test_is_not_cds(check, db: duckdb.DuckDBPyConnection):
    df = pl.DataFrame(
        {"sequence": ["ATGCCCTTTGGGTCC", "ATGCCCTTTGGGCCC", "ATGCCCTTTGGGTTT"]}
    )
    check.table_name = "df"
    db.register("df", df)
    check.bio.is_cds("sequence")
    assert check.validate(db).status.str.match("FAIL").all()


def test_is_protein(check, db: duckdb.DuckDBPyConnection):
    df = pl.DataFrame({"sequence": ["ARND", "PSTW", "GHIL"]})
    check.table_name = "df"
    db.register("df", df)
    check.bio.is_protein("sequence")
    assert check.validate(db).status.str.match("PASS").all()


def test_is_not_protein(check, db: duckdb.DuckDBPyConnection):
    df = pl.DataFrame({"sequence": ["XXX", "OO1", "UU2"]})
    check.table_name = "df"
    db.register("df", df)
    check.bio.is_protein("sequence")
    assert check.validate(db).status.str.match("FAIL").all()
