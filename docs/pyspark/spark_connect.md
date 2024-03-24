# Spark Connect

To utilize Cuallee with [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html), it's necessary to set `CUALLEE_SPARK_REMOTE` or `SPARK_REMOTE` as environment variable. The Cuallee package prioritizes `CUALLEE_SPARK_REMOTE` over `SPARK_REMOTE`.

```sh
export CUALLEE_SPARK_REMOTE="sc://localhost:15002"
# or
export SPARK_REMOTE="sc://localhost:15002"
```