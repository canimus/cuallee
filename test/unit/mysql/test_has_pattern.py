import polars as pl

from cuallee import Check

# [ ]: TODO: Fix this test
# TODO: FIX THIS TESTS

def test_positive(check: Check, db_conn_mysql):
    df = pl.DataFrame({"id": ["Herminio", "Herbert", "Harry"]})
    check.has_pattern("id", r"^H.*")
    result = check.validate(db_conn_mysql).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_is_legit(check: Check, db_conn_mysql):
    df = pl.DataFrame({"id": ["Herminio", "Herbert", "Harry"]})
    check.is_legit("id")
    result = check.validate(db_conn_mysql).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


def test_is_not_legit(check: Check, db_conn_mysql):
    df = pl.DataFrame({"id": ["Herminio", "Herbert", ""]})
    check.is_legit("id")
    result = check.validate(db_conn_mysql).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())
