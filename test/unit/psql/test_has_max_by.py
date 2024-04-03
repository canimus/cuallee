import pytest
import polars as pl

from cuallee import Check

# [ ]: has_max_by

@pytest.mark.skip(reason="Not implemented yet!")
def test_positive(check: Check, postgresql, db_conn_psql):
    check.has_max_by("id", "id2", 50)
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


@pytest.mark.skip(reason="Not implemented yet!")
def test_negative(check: Check, postgresql, db_conn_psql):
    check.has_max_by("id", "id2", 30)
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "FAIL" ).to_series().all()


@pytest.mark.skip(reason="Not implemented yet!")
@pytest.mark.parametrize(
    "answer, columns",
    [(20, [10, 20]), ("herminio", ["antoine", "herminio"])],
    ids=("numeric", "string"),
)
def test_values(check: Check, answer, columns, postgresql, db_conn_psql):
    check.has_max_by("id", "id2", answer)
    df = pl.DataFrame({"id": columns, "id2": [300, 500]})
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()


@pytest.mark.skip(reason="Not implemented yet!")
def test_coverage(check: Check, postgresql, db_conn_psql):
    with pytest.raises(TypeError):
        check.has_max_by("id", "id2", 20, 100)
