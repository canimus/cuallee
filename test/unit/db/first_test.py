
def test_postgres_docker(postgresql, db_conn):
    """Run test."""
    assert db_conn(query=f"SELECT count(*) FROM public.test1").item(0,0) == 5