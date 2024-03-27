# Check

## daft

### is_complete

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


???+ example "is_complete"

    === "simple"

        ``` python
        import daft
        import numpy as np
        from cuallee import Check, CheckLevel

        df = daft.from_pydict({"id": np.arange(10)})
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(df)
        ```

        __Result:__

        ``` markdown
        ╭───────────────────┬────────┬───────┬─────────┬───────────┬────────────────┬───────┬─────────────┬────────┬─────────────────────┬───────┬────────────╮
        │ check             ┆ column ┆ id    ┆ level   ┆ pass_rate ┆ pass_threshold ┆ rows  ┆ rule        ┆ status ┆ timestamp           ┆ value ┆ violations │
        │ ---               ┆ ---    ┆ ---   ┆ ---     ┆ ---       ┆ ---            ┆ ---   ┆ ---         ┆ ---    ┆ ---                 ┆ ---   ┆ ---        │
        │ Utf8              ┆ Utf8   ┆ Int64 ┆ Utf8    ┆ Float64   ┆ Float64        ┆ Int64 ┆ Utf8        ┆ Utf8   ┆ Utf8                ┆ Utf8  ┆ Int64      │
        ╞═══════════════════╪════════╪═══════╪═════════╪═══════════╪════════════════╪═══════╪═════════════╪════════╪═════════════════════╪═══════╪════════════╡
        │ CompletePredicate ┆ id     ┆ 1     ┆ WARNING ┆ 1         ┆ 1              ┆ 10    ┆ is_complete ┆ PASS   ┆ 2024-03-26 18:55:43 ┆ N/A   ┆ 0          │
        ╰───────────────────┴────────┴───────┴─────────┴───────────┴────────────────┴───────┴─────────────┴────────┴─────────────────────┴───────┴────────────╯
        ```

    === "coverage"

        ``` python
        import daft
        import numpy as np
        from cuallee import Check, CheckLevel

        df = daft.from_pydict({"id": np.arange(10)})
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id", .5)        # Only 50% coverage

        # Validate
        check.validate(df)
        ```

        __Result:__

        ``` markdown
        ╭───────────────────┬────────┬───────┬─────────┬───────────┬────────────────┬───────┬─────────────┬────────┬─────────────────────┬───────┬────────────╮
        │ check             ┆ column ┆ id    ┆ level   ┆ pass_rate ┆ pass_threshold ┆ rows  ┆ rule        ┆ status ┆ timestamp           ┆ value ┆ violations │
        │ ---               ┆ ---    ┆ ---   ┆ ---     ┆ ---       ┆ ---            ┆ ---   ┆ ---         ┆ ---    ┆ ---                 ┆ ---   ┆ ---        │
        │ Utf8              ┆ Utf8   ┆ Int64 ┆ Utf8    ┆ Float64   ┆ Float64        ┆ Int64 ┆ Utf8        ┆ Utf8   ┆ Utf8                ┆ Utf8  ┆ Int64      │
        ╞═══════════════════╪════════╪═══════╪═════════╪═══════════╪════════════════╪═══════╪═════════════╪════════╪═════════════════════╪═══════╪════════════╡
        │ CompletePredicate ┆ id     ┆ 1     ┆ WARNING ┆ 1         ┆ 0.5            ┆ 10    ┆ is_complete ┆ PASS   ┆ 2024-03-26 18:58:23 ┆ N/A   ┆ 0          │
        ╰───────────────────┴────────┴───────┴─────────┴───────────┴────────────────┴───────┴─────────────┴────────┴─────────────────────┴───────┴────────────╯
        ```

## are_complete

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "are_complete"

    === "simple"

        ``` python
        import daft
        from cuallee import Check, CheckLevel

        df = daft.from_pydict({"id": [10, 20], "id2": [300, 500]})
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.are_complete(("id", "id2"))

        # Validate
        check.validate(df)
        ```

        __Result:__

        ``` markdown
        ╭───────────────────┬────────────┬───────┬─────────┬───────────┬────────────┬──────────────┬────────┬─────────────────────┬───────┬────────────╮
        │ check             ┆ column     ┆ id    ┆ level   ┆ pass_rate ┆      …     ┆ rule         ┆ status ┆ timestamp           ┆ value ┆ violations │
        │ ---               ┆ ---        ┆ ---   ┆ ---     ┆ ---       ┆            ┆ ---          ┆ ---    ┆ ---                 ┆ ---   ┆ ---        │
        │ Utf8              ┆ List[Utf8] ┆ Int64 ┆ Utf8    ┆ Float64   ┆ (2 hidden) ┆ Utf8         ┆ Utf8   ┆ Utf8                ┆ Utf8  ┆ Float64    │
        ╞═══════════════════╪════════════╪═══════╪═════════╪═══════════╪════════════╪══════════════╪════════╪═════════════════════╪═══════╪════════════╡
        │ CompletePredicate ┆ [id, id2]  ┆ 1     ┆ WARNING ┆ 1         ┆ …          ┆ are_complete ┆ PASS   ┆ 2024-03-26 18:01:45 ┆ N/A   ┆ 0          │
        ╰───────────────────┴────────────┴───────┴─────────┴───────────┴────────────┴──────────────┴────────┴─────────────────────┴───────┴────────────╯
        ```

    === "coverage"

        ``` python
        import daft
        from cuallee import Check, CheckLevel

        df = daft.from_pydict({"id": [10, 20], "id2": [300, 500]})
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.are_complete(("id", "id2"), .5)         # Only 50% coverage

        # Validate
        check.validate(df)
        ```

        __Result:__

        ``` markdown
        ╭───────────────────┬────────────┬───────┬─────────┬───────────┬────────────┬──────────────┬────────┬─────────────────────┬───────┬────────────╮
        │ check             ┆ column     ┆ id    ┆ level   ┆ pass_rate ┆      …     ┆ rule         ┆ status ┆ timestamp           ┆ value ┆ violations │
        │ ---               ┆ ---        ┆ ---   ┆ ---     ┆ ---       ┆            ┆ ---          ┆ ---    ┆ ---                 ┆ ---   ┆ ---        │
        │ Utf8              ┆ List[Utf8] ┆ Int64 ┆ Utf8    ┆ Float64   ┆ (2 hidden) ┆ Utf8         ┆ Utf8   ┆ Utf8                ┆ Utf8  ┆ Float64    │
        ╞═══════════════════╪════════════╪═══════╪═════════╪═══════════╪════════════╪══════════════╪════════╪═════════════════════╪═══════╪════════════╡
        │ CompletePredicate ┆ [id, id2]  ┆ 1     ┆ WARNING ┆ 1         ┆ …          ┆ are_complete ┆ PASS   ┆ 2024-03-26 18:03:42 ┆ N/A   ┆ 0          │
        ╰───────────────────┴────────────┴───────┴─────────┴───────────┴────────────┴──────────────┴────────┴─────────────────────┴───────┴────────────╯
        ```

## is_unique

This check verifies that the number of distinct values for a column is equal to the number of rows of the dataframe.

??? example "is_unique"

    === "simple"

        ``` python
        import daft
        from cuallee import Check, CheckLevel

        df = daft.from_pydict({"id": [10, 20, 30]})
        check = Check(CheckLevel.WARNING, "IsUnique")
        check.is_unique("id")

        # Validate
        check.validate(df)
        ```

        __Result:__

        ``` markdown
        ╭──────────┬────────┬───────┬─────────┬───────────┬────────────────┬───────┬───────────┬────────┬─────────────────────┬───────┬────────────╮
        │ check    ┆ column ┆ id    ┆ level   ┆ pass_rate ┆ pass_threshold ┆ rows  ┆ rule      ┆ status ┆ timestamp           ┆ value ┆ violations │
        │ ---      ┆ ---    ┆ ---   ┆ ---     ┆ ---       ┆ ---            ┆ ---   ┆ ---       ┆ ---    ┆ ---                 ┆ ---   ┆ ---        │
        │ Utf8     ┆ Utf8   ┆ Int64 ┆ Utf8    ┆ Float64   ┆ Float64        ┆ Int64 ┆ Utf8      ┆ Utf8   ┆ Utf8                ┆ Utf8  ┆ Float64    │
        ╞══════════╪════════╪═══════╪═════════╪═══════════╪════════════════╪═══════╪═══════════╪════════╪═════════════════════╪═══════╪════════════╡
        │ IsUnique ┆ id     ┆ 1     ┆ WARNING ┆ 1         ┆ 1              ┆ 3     ┆ is_unique ┆ PASS   ┆ 2024-03-26 18:12:27 ┆ N/A   ┆ 0          │
        ╰──────────┴────────┴───────┴─────────┴───────────┴────────────────┴───────┴───────────┴────────┴─────────────────────┴───────┴────────────╯
        ```

    === "coverage"

        ``` python
        import daft
        from cuallee import Check, CheckLevel

        df = daft.from_pydict({"id": [10, 20, 30, 10]})
        check = Check(CheckLevel.WARNING, "IsUnique")
        check.is_unique("id", .5) # Only 50% coverage

        # Validate
        check.validate(df)
        ```

        __Result:__

        ``` markdown
        ╭──────────┬────────┬───────┬─────────┬───────────┬────────────────┬───────┬───────────┬────────┬─────────────────────┬───────┬────────────╮
        │ check    ┆ column ┆ id    ┆ level   ┆ pass_rate ┆ pass_threshold ┆ rows  ┆ rule      ┆ status ┆ timestamp           ┆ value ┆ violations │
        │ ---      ┆ ---    ┆ ---   ┆ ---     ┆ ---       ┆ ---            ┆ ---   ┆ ---       ┆ ---    ┆ ---                 ┆ ---   ┆ ---        │
        │ Utf8     ┆ Utf8   ┆ Int64 ┆ Utf8    ┆ Float64   ┆ Float64        ┆ Int64 ┆ Utf8      ┆ Utf8   ┆ Utf8                ┆ Utf8  ┆ Float64    │
        ╞══════════╪════════╪═══════╪═════════╪═══════════╪════════════════╪═══════╪═══════════╪════════╪═════════════════════╪═══════╪════════════╡
        │ IsUnique ┆ id     ┆ 1     ┆ WARNING ┆ 0.75      ┆ 0.5            ┆ 4     ┆ is_unique ┆ PASS   ┆ 2024-03-26 18:13:42 ┆ N/A   ┆ 1          │
        ╰──────────┴────────┴───────┴─────────┴───────────┴────────────────┴───────┴───────────┴────────┴─────────────────────┴───────┴────────────╯
        ```

## are_unique

This check is the most popular. It validates the _completeness_ attribute of a data set. It confirms that all fields contain values different of `null`.


??? example "are_unique"

    === "simple"

        ``` python
        import daft

        from cuallee import Check, CheckLevel

        df = daft.from_pydict({"id": [10, 20], "id2": [300, 500]})
        check = Check(CheckLevel.WARNING, "AreUnique")
        check.are_unique(("id", "id2"))

        # Validate
        check.validate(df)
        ```

        __Result:__

        ``` markdown
        ╭───────────────────┬────────────┬───────┬─────────┬───────────┬────────────────┬───────┬────────────┬────────┬─────────────────────┬───────┬────────────╮
        │ check             ┆ column     ┆ id    ┆ level   ┆ pass_rate ┆ pass_threshold ┆ rows  ┆ rule       ┆ status ┆ timestamp           ┆ value ┆ violations │
        │ ---               ┆ ---        ┆ ---   ┆ ---     ┆ ---       ┆ ---            ┆ ---   ┆ ---        ┆ ---    ┆ ---                 ┆ ---   ┆ ---        │
        │ Utf8              ┆ List[Utf8] ┆ Int64 ┆ Utf8    ┆ Float64   ┆ Float64        ┆ Int64 ┆ Utf8       ┆ Utf8   ┆ Utf8                ┆ Utf8  ┆ Float64    │
        ╞═══════════════════╪════════════╪═══════╪═════════╪═══════════╪════════════════╪═══════╪════════════╪════════╪═════════════════════╪═══════╪════════════╡
        │ CompletePredicate ┆ [id, id2]  ┆ 1     ┆ WARNING ┆ 1         ┆ 1              ┆ 2     ┆ are_unique ┆ PASS   ┆ 2024-03-26 18:13:42 ┆ N/A   ┆ 0          │
        ╰───────────────────┴────────────┴───────┴─────────┴───────────┴────────────────┴───────┴────────────┴────────┴─────────────────────┴───────┴────────────╯
        ```

    === "coverage"

        ``` python
        import daft
        from cuallee import Check, CheckLevel

        df = daft.from_pydict({"id": [10, None], "id2": [300, 500]})
        check = Check(CheckLevel.WARNING, "AreUnique")
        check.are_unique(("id", "id2"), 0.5)  # Only 50% coverage

        # Validate
        check.validate(df)
        ```

        __Result:__

        ``` markdown
        ╭───────────────────┬────────────┬───────┬─────────┬───────────┬────────────────┬───────┬────────────┬────────┬─────────────────────┬───────┬────────────╮
        │ check             ┆ column     ┆ id    ┆ level   ┆ pass_rate ┆ pass_threshold ┆ rows  ┆ rule       ┆ status ┆ timestamp           ┆ value ┆ violations │
        │ ---               ┆ ---        ┆ ---   ┆ ---     ┆ ---       ┆ ---            ┆ ---   ┆ ---        ┆ ---    ┆ ---                 ┆ ---   ┆ ---        │
        │ Utf8              ┆ List[Utf8] ┆ Int64 ┆ Utf8    ┆ Float64   ┆ Float64        ┆ Int64 ┆ Utf8       ┆ Utf8   ┆ Utf8                ┆ Utf8  ┆ Float64    │
        ╞═══════════════════╪════════════╪═══════╪═════════╪═══════════╪════════════════╪═══════╪════════════╪════════╪═════════════════════╪═══════╪════════════╡
        │ CompletePredicate ┆ [id, id2]  ┆ 1     ┆ WARNING ┆ 0.75      ┆ 0.5            ┆ 2     ┆ are_unique ┆ PASS   ┆ 2024-03-26 18:13:42 ┆ N/A   ┆ 0.5        │
        ╰───────────────────┴────────────┴───────┴─────────┴───────────┴────────────────┴───────┴────────────┴────────┴─────────────────────┴───────┴────────────╯
        ```




