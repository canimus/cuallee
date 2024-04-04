import pytest
import polars as pl

from cuallee import Check

# [ ]: has_entropy

@pytest.mark.skip(reason="Not implemented yet!")
def test_positive(check: Check, postgresql, db_conn_psql):
    check.has_entropy("id", 1.0)
    check.table_name = "public.test3"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()

@pytest.mark.skip(reason="Not implemented yet!")
def test_negative(check: Check, postgresql, db_conn_psql):
    check.has_entropy("id", 1.0)
    df = pl.DataFrame({"id": [10, 10, 10, 10, 50]})
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


@pytest.mark.skip(reason="Not implemented yet!")
@pytest.mark.parametrize(
    "values", [[1], [1, 1, 1, 1, 1]], ids=("observation", "classes")
)
def test_parameters(check: Check, values, postgresql, db_conn_psql):
    check.has_entropy("id", 0.0)
    df = pl.DataFrame({"id": values})
    result = check.validate(df).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


@pytest.mark.skip(reason="Not implemented yet!")
def test_coverage(check: Check, postgresql, db_conn_psql):
    with pytest.raises(TypeError):
        check.has_entropy("id", 1.0, pct=0.5)
