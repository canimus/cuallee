# How To Setup Test Environment
This guide simplifies the process of unit testing `cuallee` with `PyTest`.

## PostgreSQL

For unit testing `PostgreSQL` functionality of `cuallee`, you can use Docker. 

To initiate a `PostgreSQL` database via Docker, execute the following command:
```sh
docker run -e POSTGRES_PASSWORD="another!!22TEST" -p 5432:5432 -d --name psql_test_db postgres:alpine3.18
```
This will create a container with name `psql_test_db` and user `postgres` with password `another!!22TEST`. Once the database is created, you can use it to run tests.

The project integrates `pytest-postgresql` to facilitate testing. This tool provides a `PyTest` fixture that establishes a connection to your PostgreSQL database, loads test data from `./test/unit/mysql/init-db-mysql.sql`, and ensures proper cleanup after tests conclude.

Simply incorporate this fixture into your test cases to automatically leverage the configured database.

```python
# `conftest.py`
from pytest_postgresql import factories as postgres_factories

postgresql_in_docker = postgres_factories.postgresql_noproc(host="localhost", user= "postgres", password="another!!22TEST", dbname="testdb")
postgresql = postgres_factories.postgresql("postgresql_in_docker", load=[Path("./test/unit/psql/init-db-psql.sql")])

@pytest.fixture(scope="session")
def db_conn_psql():
    uri = "postgresql://postgres:another!!22TEST@localhost/testdb"
    yield db_connector(uri)

# `tests_is_unique.py`
def test_positive(check: Check, postgresql, db_conn_psql):
    check.is_unique("id")
    check.table_name = "public.test1"
    result = check.validate(db_conn_psql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
```

## MySQL

For unit testing `MySQL` functionality of `cuallee`, you can use Docker. 

To initiate a `MySQL` database via Docker, execute the following command:
```sh
docker run -e MYSQL_ROOT_PASSWORD="another!!22TEST" -p 3306:3306 -d --name myql_test_db mysql:latest
```
This will create a container with name `myql_test_db` and user `root` with password `another!!22TEST`. Once the database is created, you can use it to run tests.

The project integrates `pytest_mysql` to facilitate testing. This tool provides a `PyTest` fixture that establishes a connection to your PostgreSQL database, and ensures proper cleanup after tests conclude. `db_conn_mysql` will load test data from `./test/unit/mysql/init-db-mysql.sql`.

Simply incorporate this fixture into your test cases to automatically leverage the configured database.

```python
# `conftest.py`
from pytest_mysql import factories as mysql_factories

mysql_in_docker = mysql_factories.mysql_noproc(host="localhost", user= "root")
mysql = mysql_factories.mysql("mysql_in_docker", passwd="another!!22TEST", dbname="public")

@pytest.fixture()
def db_conn_mysql(request):

    mysql = request.getfixturevalue("mysql")
    try:
        with open(Path("./test/unit/mysql/init-db-mysql.sql"), 'r') as f:
            with mysql.cursor() as cur:
                cur.execute(f.read())
                mysql.commit()
                cur.close()
    except:  # noqa: E722
        pass

    uri = "mysql://root:another!!22TEST@localhost"
    yield db_connector(uri)

# `tests_is_unique.py`
def test_positive(check: Check, db_conn_mysql):
    check.is_unique("id")
    check.table_name = "public.test1"
    result = check.validate(db_conn_mysql)
    assert (result.select(pl.col("status")) == "PASS" ).to_series().all()
```