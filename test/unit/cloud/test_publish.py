from cuallee.cloud import publish, compress, CUALLEE_CLOUD_HEADERS
from unittest.mock import patch
import os


def test_publish(spark, check):
    os.environ["CUALLEE_CLOUD_HOST"] = "https://localhost:5000/msgpack"
    os.environ["CUALLEE_CLOUD_TOKEN"] = "test"
    df = spark.range(10)
    check.is_complete("id")
    with patch("cuallee.cloud.requests.post") as mock_post:
        mock_post.return_value.status_code = 200
        check.validate(df)
        mock_post.assert_called_once_with(
            "https://localhost:5000/msgpack",
            data=compress(check),
            headers=CUALLEE_CLOUD_HEADERS,
            verify=False,
        )


def test_connection(spark, check):
    os.environ["CUALLEE_CLOUD_HOST"] = "https://localhost:6000/wrong"
    os.environ["CUALLEE_CLOUD_TOKEN"] = "test"
    df = spark.range(10)
    check.is_complete("id")
    check.validate(df)
