import os
import pytest
import duckdb
import logging

from pathlib import Path
from pyspark.sql import SparkSession
from pytest_mysql import factories as mysql_factories
from pytest_postgresql import factories as postgres_factories

from cuallee import Check, CheckLevel, db_connector


logger = logging.getLogger(__name__)

try:
    from snowflake.snowpark import Session  # type: ignore
except:
    print("No snowflake available")

try:
    from google.cloud import bigquery
except:
    print("No BigQuery Client available")


@pytest.fixture(scope="session")
def spark():
    try:
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.ERROR)
        spark_session = SparkSession.builder.config(
            "spark.driver.memory", "2g"
        ).getOrCreate()
        yield spark_session
    except:
        pass
    finally:
        spark_session.stop()


@pytest.fixture(scope="function")
def check():
    return Check(CheckLevel.WARNING, "PyTestCheck")


@pytest.fixture(scope="session")
def configurations():
    return {
        "account": os.getenv("SF_ACCOUNT"),
        "user": os.getenv("SF_USER"),
        "password": os.getenv("SF_PASSWORD"),
        "role": os.getenv("SF_ROLE"),
        "warehouse": os.getenv("SF_WAREHOUSE"),
        "database": os.getenv("SF_DATABASE"),
        "schema": os.getenv("SF_SCHEMA"),
    }


@pytest.fixture(scope="session")
def snowpark():
    settings = {
        "account": os.getenv("SF_ACCOUNT"),
        "user": os.getenv("SF_USER"),
        "password": os.getenv("SF_PASSWORD"),
        "role": os.getenv("SF_ROLE"),
        "warehouse": os.getenv("SF_WAREHOUSE"),
        "database": os.getenv("SF_DATABASE"),
        "schema": os.getenv("SF_SCHEMA"),
    }
    try:
        snowpark_session = Session.builder.configs(settings).create()
        yield snowpark_session
    except:
        pass


@pytest.fixture(scope="function")
def db() -> duckdb.DuckDBPyConnection: # type: ignore
    try:
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE TEMP AS SELECT UNNEST(RANGE(10)) AS ID")
        yield conn
    except:
        logger.error("Unable to init duckdb")
    finally:
        conn.close()


@pytest.fixture(scope="session")
def bq_client():

    if Path('temp/key.json').exists()==True:
        credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    else:
        from google.oauth2 import service_account
        import os
        import json
        with open("key.json", "w") as writer:
            json.dump(json.loads(os.getenv("GOOGLE_KEY")), writer)

        credentials = service_account.Credentials.from_service_account_file("key.json")

    try:
        client = bigquery.Client(project="cuallee-bigquery-386709", credentials=credentials)
        return client
    except:
        pass
    #finally:
        #client.stop()


postgresql_in_docker = postgres_factories.postgresql_noproc(host="localhost", user= "postgres", password="another!!22TEST", dbname="testdb")
postgresql = postgres_factories.postgresql("postgresql_in_docker", load=[Path("./test/unit/psql/init-db.sql")])

# mysql_in_docker = mysql_factories.mysql_noproc(host="localhost", user= "root")
# mysql = mysql_factories.mysql("mysql_in_docker", passwd="another!!22TEST", dbname="public")

# @pytest.fixture(params=["postgresql", "mysql"], ids=["PostgreSQL", "MySQL"])
# def db_conn(request):

#     fixture_name = request.param

#     # Connect to the PostgreSQL database
#     if fixture_name == 'postgresql':
#         request.getfixturevalue(fixture_name)
#         # Connect to the PostgreSQL database
#         uri = "postgresql://postgres:another!!22TEST@localhost/testdb"
#         yield db_connector(uri)


#     # Connect to the MySQL database
#     elif fixture_name == 'mysql':
#         mysql = request.getfixturevalue(fixture_name)
#         try:
#             with open(Path("./test/unit/db/init-db.sql"), 'r') as f:
#                 cur = mysql.cursor()
#                 cur.execute(f.read())
#                 # mysql.commit()
#                 # cur.close()
#                 uri = "mysql://root:another!!22TEST@localhost/public"
#                 yield db_connector(uri)
#         except:
#             pass


@pytest.fixture(scope="session")
def db_conn():
    uri = "postgresql://postgres:another!!22TEST@localhost/testdb"
    yield db_connector(uri)