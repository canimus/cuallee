# Installation

`cuallee` is developed with a `functional` programming style. Classes are only defined for  compatibility and ease the migration process from `pydeeque`. 

For better performance it requires `pyspark>=3.3.0` and the `Observation` API.


## Requirements

`cuallee` is available independently of your operative system, you can run `cuallee` on:

- `Linux` :material-penguin:
- `MacOS` :material-apple:
- `Windows` :material-microsoft-windows:

The binaries in the python `wheel` are built universally so as far as you have a python interpreter with the minimum version `>=3.8` you are all set.


## Using pip

```bash
# Latest
pip install cuallee
```

## External frameworks
If you are operating in an environment and your frameworks are already installed, then there is no need or requirement to install them separately. If you are starting from scratch and you choose for `cuallee` to install the compatible or supported versions of the data frameworks, you could choose from the following `optional-dependencies` available in `cuallee`.

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

# For Snowpark
pip install cuallee[snowpark]
```
Alternatively, you can have your own versions of the frameworks installed separately in your environment and `cuallee` simply will rely in the version installed, considering it meets its minimum requirements and compatibility.

