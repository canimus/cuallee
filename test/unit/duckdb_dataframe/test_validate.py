import duckdb

from cuallee import Check, CheckLevel
from cuallee.duckdb_validation import compute


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    assert compute(check)
