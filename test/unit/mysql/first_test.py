
def test_mysql_docker(db_conn_mysql):
    """Run test."""
    assert db_conn_mysql(query="SELECT count(*) FROM public.test1").item(0,0) == 5
    assert db_conn_mysql(query="SELECT count(*) FROM public.test3").item(0,0) == 6
    assert db_conn_mysql(query="SELECT count(*) FROM public.test7").item(0,0) == 3
