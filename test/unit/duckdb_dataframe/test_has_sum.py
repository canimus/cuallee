import duckdb
import pytest

from cuallee import Check


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_sum("id", 45)
    check.table_name = "TEMP"
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_sum("id", 40)
    check.table_name = "TEMP"
    assert check.validate(db).status.str.match("FAIL").all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_sum("id", 10, 0.5)
