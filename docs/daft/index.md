In order to follow this examples, make sure your installation is all set for `daft`

!!! success "Install"
    ``` sh
    pip install cuallee
    pip install cuallee[daft]
    ```


### is_complete

It validates the _completeness_ attribute of a data set. It confirms that a column does not contain `null` values.



???+ example "is_complete"

    === ":material-checkbox-marked-circle:{ .ok } PASS"

        In this example, we validate that the column `id` does not have any missing values.

        ``` py
        import daft
        import numpy as np
        from cuallee import Check, CheckLevel

        df = daft.from_pydict({"id": np.arange(10)})
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(df)
        ```

        :material-export: __output:__ 

        ``` markdown
        ╭───────────────────┬────────┬───────┬─────────┬───────────┬────────────────┬───────┬─────────────┬────────┬─────────────────────┬───────┬────────────╮
        │ check             ┆ column ┆ id    ┆ level   ┆ pass_rate ┆ pass_threshold ┆ rows  ┆ rule        ┆ status ┆ timestamp           ┆ value ┆ violations │
        │ ---               ┆ ---    ┆ ---   ┆ ---     ┆ ---       ┆ ---            ┆ ---   ┆ ---         ┆ ---    ┆ ---                 ┆ ---   ┆ ---        │
        │ Utf8              ┆ Utf8   ┆ Int64 ┆ Utf8    ┆ Float64   ┆ Float64        ┆ Int64 ┆ Utf8        ┆ Utf8   ┆ Utf8                ┆ Utf8  ┆ Int64      │
        ╞═══════════════════╪════════╪═══════╪═════════╪═══════════╪════════════════╪═══════╪═════════════╪════════╪═════════════════════╪═══════╪════════════╡
        │ CompletePredicate ┆ id     ┆ 1     ┆ WARNING ┆ 1         ┆ 1              ┆ 10    ┆ is_complete ┆ PASS   ┆ 2024-03-26 18:55:43 ┆ N/A   ┆ 0          │
        ╰───────────────────┴────────┴───────┴─────────┴───────────┴────────────────┴───────┴─────────────┴────────┴─────────────────────┴───────┴────────────╯
        ```

    === ":material-close-circle:{ .ko } FAIL"

        In this example, we intentionally place 2 `null` values in the dataframe and that produces a `FAIL` check as result.

        ``` python
        import daft
        import numpy as np
        from cuallee import Check, CheckLevel

        df = daft.from_pydict({"id": [1,2,3,None,None]})
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id")

        # Validate
        check.validate(df)
        ```

        :material-export: __output:__ 

        ``` markdown
        ╭───────────────────┬────────┬───────┬─────────┬───────────┬────────────────┬───────┬─────────────┬────────┬─────────────────────┬───────┬────────────╮
        │ check             ┆ column ┆ id    ┆ level   ┆ pass_rate ┆ pass_threshold ┆ rows  ┆ rule        ┆ status ┆ timestamp           ┆ value ┆ violations │
        │ ---               ┆ ---    ┆ ---   ┆ ---     ┆ ---       ┆ ---            ┆ ---   ┆ ---         ┆ ---    ┆ ---                 ┆ ---   ┆ ---        │
        │ Utf8              ┆ Utf8   ┆ Int64 ┆ Utf8    ┆ Float64   ┆ Float64        ┆ Int64 ┆ Utf8        ┆ Utf8   ┆ Utf8                ┆ Utf8  ┆ Int64      │
        ╞═══════════════════╪════════╪═══════╪═════════╪═══════════╪════════════════╪═══════╪═════════════╪════════╪═════════════════════╪═══════╪════════════╡
        │ CompletePredicate ┆ id     ┆ 1     ┆ WARNING ┆ 0.6       ┆ 1              ┆ 5     ┆ is_complete ┆ FAIL   ┆ 2024-05-18 21:24:15 ┆ N/A   ┆ 2          │
        ╰───────────────────┴────────┴───────┴─────────┴───────────┴────────────────┴───────┴─────────────┴────────┴─────────────────────┴───────┴────────────╯

        ```

    === ":material-alert-circle:{ .kk } THRESHOLD"

        In this example, we validate reuse the data frame with empty values from the previous example, however we set our tolerance via the `pct` parameter on the rule `is_complete` to `0.6`. Producing now a `PASS` result on the check, regardless of the `2` present `null` values.

        ``` python
        import daft
        import numpy as np
        from cuallee import Check, CheckLevel

        df = daft.from_pydict({"id": [1,2,3,None,None]})
        check = Check(CheckLevel.WARNING, "CompletePredicate")
        check.is_complete("id", pct=.6)

        # Validate
        check.validate(df)
        ```

        :material-export: __output:__ 

        ``` markdown
        ╭───────────────────┬────────┬───────┬─────────┬───────────┬────────────────┬───────┬─────────────┬────────┬─────────────────────┬───────┬────────────╮
        │ check             ┆ column ┆ id    ┆ level   ┆ pass_rate ┆ pass_threshold ┆ rows  ┆ rule        ┆ status ┆ timestamp           ┆ value ┆ violations │
        │ ---               ┆ ---    ┆ ---   ┆ ---     ┆ ---       ┆ ---            ┆ ---   ┆ ---         ┆ ---    ┆ ---                 ┆ ---   ┆ ---        │
        │ Utf8              ┆ Utf8   ┆ Int64 ┆ Utf8    ┆ Float64   ┆ Float64        ┆ Int64 ┆ Utf8        ┆ Utf8   ┆ Utf8                ┆ Utf8  ┆ Int64      │
        ╞═══════════════════╪════════╪═══════╪═════════╪═══════════╪════════════════╪═══════╪═════════════╪════════╪═════════════════════╪═══════╪════════════╡
        │ CompletePredicate ┆ id     ┆ 1     ┆ WARNING ┆ 0.6       ┆ 0.6            ┆ 5     ┆ is_complete ┆ PASS   ┆ 2024-05-18 21:24:15 ┆ N/A   ┆ 2          │
        ╰───────────────────┴────────┴───────┴─────────┴───────────┴────────────────┴───────┴─────────────┴────────┴─────────────────────┴───────┴────────────╯
        ```

