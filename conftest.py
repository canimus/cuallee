import os
import warnings
import pytest
from cuallee import Check, CheckLevel
from pyspark.sql import SparkSession
from snowflake.snowpark import Session # type: ignore
import logging

@pytest.fixture(scope="session")
def spark():
    try:
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.ERROR)
        spark_session = (
            SparkSession.builder
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )
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
    	"account" : os.getenv("SF_ACCOUNT"),
    	"user" : os.getenv("SF_USER"),
    	"password" : os.getenv("SF_PASSWORD"),
    	"role" : os.getenv("SF_ROLE"),
    	"warehouse" : os.getenv("SF_WAREHOUSE"),
    	"database" : os.getenv("SF_DATABASE"),
    	"schema" : os.getenv("SF_SCHEMA")
	}

@pytest.fixture(scope="session")
def snowpark():
	settings = {
    	"account" : os.getenv("SF_ACCOUNT"),
    	"user" : os.getenv("SF_USER"),
    	"password" : os.getenv("SF_PASSWORD"),
    	"role" : os.getenv("SF_ROLE"),
    	"warehouse" : os.getenv("SF_WAREHOUSE"),
    	"database" : os.getenv("SF_DATABASE"),
    	"schema" : os.getenv("SF_SCHEMA")
	}
	try:
		snowpark_session = Session.builder.configs(settings).create()
		yield snowpark_session
	except:
		pass
