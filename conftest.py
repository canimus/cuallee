import os
import pytest
import duckdb
import logging

from pathlib import Path
from pyspark.sql import SparkSession
from pytest_postgresql import factories

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


postgresql_in_docker = factories.postgresql_noproc(host="localhost", user= "postgres", password="another!!22TEST", dbname="testdb")
postgresql = factories.postgresql("postgresql_in_docker", load=[Path("./test/unit/db/init-db.sql")])

@pytest.fixture(scope="session")
def db_conn():
    uri = "postgresql://postgres:another!!22TEST@localhost/testdb"
    return db_connector(uri)