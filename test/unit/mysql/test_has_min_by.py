import polars as pl
from cuallee import Check
import pytest

# [ ]: has_min_by

@pytest.mark.skip(reason="Not implemented yet!")
def test_positive(check: Check, db_conn_psql):
    check.has_min_by("id", "id2", 300)
    df = pl.DataFrame({"id": [10, 20], "id2": [300, 500]})
    result = check.validate(db_conn_psql).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())

@pytest.mark.skip(reason="Not implemented yet!")
def test_negative(check: Check, db_conn_psql):
    check.has_min_by("id", "id2", 50)
    df = pl.DataFrame({"id": [10, 20], "id2": [300, 500]})
    result = check.validate(db_conn_psql).select(pl.col("status")) == "FAIL"
    assert all(result.to_series().to_list())


@pytest.mark.skip(reason="Not implemented yet!")
@pytest.mark.parametrize(
    "answer, columns",
    [(10, [10, 20]), ("antoine", ["antoine", "herminio"])],
    ids=("numeric", "string"),
)
def test_values(check: Check, answer, columns, db_conn_psql):
    check.has_min_by("id2", "id", answer)
    result = check.validate(db_conn_psql).select(pl.col("status")) == "PASS"
    assert all(result.to_series().to_list())


@pytest.mark.skip(reason="Not implemented yet!")
def test_coverage(check: Check, db_conn_psql):
    with pytest.raises(TypeError):
        check.has_min_by("id2", "id", 20, 100)