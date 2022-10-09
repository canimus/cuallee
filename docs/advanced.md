# Advanced Checks

## satisfies

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


???+ example "satisfies"

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

## has_entropy

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "has_entropy"

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

## has_weekday_continuity

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "has_weekday_continuity"

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