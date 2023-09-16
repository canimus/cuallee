from cuallee import Check, CheckLevel
import duckdb
from cuallee.duckdb_validation import compute


def test_positive(check: Check, db: duckdb.DuckDBPyConnection):
    assert compute(check)
