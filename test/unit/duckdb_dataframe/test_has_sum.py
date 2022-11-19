import duckdb
from cuallee import Check
import pytest

def test_positive(check: Check):    
    check.has_sum("id", 45)
    check.table_name = "TEMP"
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE TEMP AS SELECT UNNEST(RANGE(10)) AS ID")
    assert check.validate(conn).status.str.match("PASS").all()

def test_negative(check: Check):
    check.has_sum("id", 40)
    check.table_name = "TEMP"
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE TEMP AS SELECT UNNEST(RANGE(10)) AS ID")
    assert check.validate(conn).status.str.match("FAIL").all()

def test_coverage(check: Check):
    with pytest.raises(TypeError):
        check.has_sum("id", 10, 0.5)