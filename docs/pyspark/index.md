# :simple-apachespark: PySpark
In order to follow this examples, make sure your installation is all set for `pyspark`

!!! success "Install"
    ``` sh
    pip install cuallee
    pip install cuallee[pyspark]
    ```


### is_complete

It validates the _completeness_ attribute of a data set. It confirms that a column does not contain `null` values.


???+ example "is_complete"

    === ":material-checkbox-marked-circle:{ .ok } PASS"

        In this example, we validate that the column `id` does not have any missing values.

        ``` py
        from pyspark.sql import SparkSession
        from cuallee import Check
        
        spark = SparkSession.builder.getOrCreate()
        df = spark.range(10)
        check = Check()
        check.is_complete("id")

        # Validate
        check.validate(df).show()
        ```

        :material-export: __output:__ 

        ``` markdown
        +---+-------------------+-------------+-------+------+-----------+-----+----+----------+---------+--------------+------+
        | id|          timestamp|        check|  level|column|       rule|value|rows|violations|pass_rate|pass_threshold|status|
        +---+-------------------+-------------+-------+------+-----------+-----+----+----------+---------+--------------+------+
        |  1|2024-05-18 16:53:56|cuallee.check|WARNING|    id|is_complete|  N/A|  10|         0|      1.0|           1.0|  PASS|
        +---+-------------------+-------------+-------+------+-----------+-----+----+----------+---------+--------------+------+
        ```

    === ":material-close-circle:{ .ko } FAIL"

        In this example, we intentionally place 2 `null` values in the dataframe and that produces a `FAIL` check as result.

        ``` py
        from pyspark.sql import SparkSession
        from cuallee import Check
        
        spark = SparkSession.builder.getOrCreate()
        df = spark.range(8).union(spark.createDataFrame([(None,), (None,)], schema="id int"))
        check = Check()
        check.is_complete("id")

        # Validate
        check.validate(df).show()
        ```

        :material-export: __output:__ 

        ``` markdown
        +---+-------------------+-------------+-------+------+-----------+-----+----+----------+---------+--------------+------+
        | id|          timestamp|        check|  level|column|       rule|value|rows|violations|pass_rate|pass_threshold|status|
        +---+-------------------+-------------+-------+------+-----------+-----+----+----------+---------+--------------+------+
        |  1|2024-05-18 16:53:56|cuallee.check|WARNING|    id|is_complete|  N/A|  10|         2|      0.8|           1.0|  FAIL|
        +---+-------------------+-------------+-------+------+-----------+-----+----+----------+---------+--------------+------+
        ```

        
        
    === ":material-alert-circle:{ .kk } THRESHOLD"

        In this example, we validate reuse the data frame with empty values from the previous example, however we set our tolerance via the `pct` parameter on the rule `is_complete` to `0.8`. Producing now a `PASS` result on the check, regardless of the `2` present `null` values.

        ``` py
        from pyspark.sql import SparkSession
        from cuallee import Check
        
        spark = SparkSession.builder.getOrCreate()
        df = spark.range(8).union(spark.createDataFrame([(None,), (None,)], schema="id int"))
        check = Check()
        check.is_complete("id", pct=0.8)

        # Validate
        check.validate(df).show()
        ```

        :material-export: __output:__ 

        ``` markdown
        +---+-------------------+-------------+-------+------+-----------+-----+----+----------+---------+--------------+------+
        | id|          timestamp|        check|  level|column|       rule|value|rows|violations|pass_rate|pass_threshold|status|
        +---+-------------------+-------------+-------+------+-----------+-----+----+----------+---------+--------------+------+
        |  1|2024-05-18 16:53:56|cuallee.check|WARNING|    id|is_complete|  N/A|  10|         2|      0.8|           0.8|  PASS|
        +---+-------------------+-------------+-------+------+-----------+-----+----+----------+---------+--------------+------+
        ```

        

