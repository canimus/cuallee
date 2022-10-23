# Check

## pandas

### is_complete

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


???+ example "is_complete"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## are_complete

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "are_complete"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_unique

This check verifies that the number of distinct values for a column is equal to the number of rows of the dataframe.

??? example "is_unique"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## are_unique

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "are_unique"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_greater_than

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_greater_than"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_greater_or_equal_than

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_greater_or_equal_than"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_less_than

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_less_than"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_less_or_equal_than

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_less_or_equal_than"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_equal_than

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_equal_than"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## has_pattern

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "has_pattern"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## has_min

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "has_min"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## has_max

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "has_max"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## has_std

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "has_std"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## has_mean

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "has_mean"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_between

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_between"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_contained_in

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_contained_in"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_in

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_in"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_on_weekday

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_on_weekday"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_on_weekend

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_on_weekend"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_on_monday

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_on_monday"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_on_tuesday

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_on_tuesday"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_on_wednesday

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_on_wednesday"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_on_thursday

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_on_thursday"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

## is_on_friday

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_on_friday"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```


## is_on_saturday

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_on_saturday"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

##  is_on_sunday

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_on_sunday"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

##  is_on_schedule

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "is_on_schedule"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

##  has_percentile

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "has_percentile"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

##  has_max_by

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "has_max_by"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

##  has_min_by

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "has_min_by"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

##  has_correlation

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "has_correlation"

    === "simple"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |1.0           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```

    === "coverage"

        ``` python
        from cuallee import Check, CheckLevel
        df = spark.range(10)
        check = Check(CheckLevel.WARNING, "IsComplete")
        check.is_complete("id", .5) # Only 50% coverage

        # Validate
        check.validate(spark, df).show(truncate=False)
        ```

        __Result:__

        ``` markdown
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |id |timestamp          |check            |level  |column|rule       |value|rows|violations|pass_rate|pass_threshold|metadata|status|
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        |1  |2022-10-09 23:45:10|CompletePredicate|WARNING|id    |is_complete|N/A  |10  |0         |1.0      |0.5           |{}      |PASS  |
        +---+-------------------+-----------------+-------+------+-----------+-----+----+----------+---------+--------------+--------+------+
        ```


