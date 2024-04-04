
def test_postgres_docker(postgresql, db_conn_psql):
    """Run test."""
    assert db_conn_psql(query="SELECT count(*) FROM public.test1").item(0,0) == 5