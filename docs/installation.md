# Installation

`cuallee` is developed with a `functional` programming style. Classes are only defined for  compatibility and ease the migration process from `pydeeque`. 

For better performance it requires `pyspark>=3.3.0` and the `Observation` API.

## Using pip

```bash
# Latest
pip install cuallee
```

## External frameworks
```bash
# For Pyspark
pip install cuallee[pyspark]

# For Spark Connect
pip install cuallee[pyspark_connect]

# For Snowflake/Snowpark
pip install cuallee[snowpark]

# For DuckDb
pip install cuallee[duckdb]

# For Polars
pip install cuallee[polars]
```
Alternatively, you can have your own versions of the frameworks installed separately in your environment and `cuallee` simply will rely in the version installed, considering it meets its minimum requirements and compatibility.

