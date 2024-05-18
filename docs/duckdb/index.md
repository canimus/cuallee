# :simple-duckdb: DuckDB

In order to follow this examples, make sure your installation is all set for `duckdb`

!!! success "Install"
    ``` sh
    pip install cuallee
    pip install cuallee[duckdb]
    ```

The example illustrated in this section, uses `pandas` to create a test data structure, as opposed `DML` in `duckdb` to create a `table` or `view` to conduct the demonstration.<br/>
`duckdb` uses a `pyarrow` to access the `pandas` dataframes in memory, and therefore is capable to interpret a `pandas` dataframe as a `virtual table` from which it can run queries.<br/>
The name given to the `virtual table` is the _variable name_ in the case of the majority of the examples this is: `df` or alternatively `df2`.

You can find more information [here](https://duckdb.org/docs/guides/python/sql_on_pandas).

## Data Structures

### `DuckDBPyConnection`

`duckdb` is a rapid changing framework with a relatively fast release cadence. That means that keeping up to date with their releases is challenging. For example in version `0.10.0` the project introduced a PySpark API that allow users to interact with a DuckDB database with a very similar API as that of Spark. However, parity of the APIs is not `100%` and in practice, the most reliable API for `duckdb` is always the `SQL` interface. For that reason, in `cuallee` we decided to use that interface to `duckdb` as opposed to the one closest to the `dataframe`.

What this means, is that `cuallee` uses the following data type to interact with the data:

!!! tip "Supported"
    ``` py
    import duckdb
    conn = duckdb.connect(":memory:")
    type(conn)
    duckdb.duckdb.DuckDBPyConnection
    ```

### `DuckDBPyRelation`

`cuallee` does not support the relational API as of version `0.10.3`. Implenting it, will depend on the parity of functionality with the native `SQL` API in DuckDb. Currently not all the operations supported in the native `SQL` API of duckdb are available through the relational API, and therefore it has limitations when trying to match their siblings for instance with PySpark or Pandas.


!!! danger "Not Supported"
    ``` py
    import duckdb
    conn = duckdb.read_parquet("data.parquet")
    type(conn)
    duckdb.duckdb.DuckDBPyRelation
    ```

## Checks

### is_complete

It validates the _completeness_ attribute of a data set. It confirms that a column does not contain `null` values   .


???+ example "is_complete"

    === ":material-checkbox-marked-circle:{ .ok } PASS"

        In this example, we validate that the column `id` does not have any missing values.

        ``` py
        import pandas as pd
        import duckdb
        from cuallee import Check
        
        conn = duckdb.connect(":memory:")
        df = pd.DataFrame({"id" : [1,2,3,4,5]})
        check = Check(table_name="df")
        check.is_complete("id")

        # Validate
        check.validate(conn)
        ```

        :material-export: __output:__ 

        ``` markdown
        id            timestamp          check    level column         rule value  rows  violations  pass_rate  pass_threshold status
         1  2024-05-18 16:22:53  cuallee.check  WARNING     id  is_complete   N/A     5           0        1.0             1.0   PASS
        ```

    === ":material-close-circle:{ .ko } FAIL"

        In this example, we intentionally place 2 `null` values in the dataframe and that produces a `FAIL` check as result.

        ``` py        
        import pandas as pd
        import duckdb
        from cuallee import Check
        
        conn = duckdb.connect(":memory:")
        df = pd.DataFrame({"id" : [1,2,3,None, None]})
        check = Check(table_name="df")
        check.is_complete("id")

        # Validate
        check.validate(conn)
        ```

        :material-export: __output:__ 

        ``` markdown
        id            timestamp          check    level column         rule value  rows  violations  pass_rate  pass_threshold status
         1  2024-05-18 16:33:55  cuallee.check  WARNING     id  is_complete   N/A     5           2        0.6             1.0   FAIL
        ```

        
        
    === ":material-alert-circle:{ .kk } THRESHOLD"

        In this example, we validate reuse the data frame with empty values from the previous example, however we set our tolerance via the `pct` parameter on the rule `is_complete` to `0.6`. Producing now a `PASS` result on the check, regardless of the `2` present `null` values.

        ``` py
        import pandas as pd
        import duckdb
        from cuallee import Check
        
        conn = duckdb.connect(":memory:")
        df = pd.DataFrame({"id" : [1,2,3,None, None]})
        check = Check(table_name="df")
        check.is_complete("id", pct=0.6)

        # Validate
        check.validate(conn)
        ```

        :material-export: __output:__ 

        ``` markdown
        id            timestamp          check    level column         rule value  rows  violations  pass_rate  pass_threshold status
         1  2024-05-18 16:33:55  cuallee.check  WARNING     id  is_complete   N/A     5           2        0.6             0.6   PASS
        ```

        

