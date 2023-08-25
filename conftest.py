import os
import warnings
import pytest
from cuallee import Check, CheckLevel
from pyspark.sql import SparkSession
import logging
import duckdb

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
def db() -> duckdb.DuckDBPyConnection:
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