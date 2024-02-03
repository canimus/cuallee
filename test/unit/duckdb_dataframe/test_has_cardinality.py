import duckdb
from cuallee import Check
import pytest


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_cardinality("id", 10)
    check.table_name = "TEMP"
    assert check.validate(db).status.str.match("PASS").all()


def test_negative(check: Check, db: duckdb.DuckDBPyConnection):
    check.has_cardinality("id", 40)
    check.table_name = "TEMP"
    assert check.validate(db).status.str.match("FAIL").all()


def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_cardinality("id", 10, 0.5)
