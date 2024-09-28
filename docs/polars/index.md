# :simple-polars: Polars

In order to follow this examples, make sure your installation is all set for `polars`

!!! success "Install"
    ``` sh
    pip install cuallee
    pip install cuallee[polars]
    ```


### is_complete

It validates the _completeness_ attribute of a data set. It confirms that a column does not contain `null` values   .


???+ example "is_complete"

    === ":material-checkbox-marked-circle:{ .ok } PASS"

        In this example, we validate that the column `id` does not have any missing values.

        ``` py
        import polars as pl
        from cuallee import Check

        df = pl.DataFrame({"id" : [1,2,3,4,5]})
        check = Check()
        check.is_complete("id")

        # Validate
        check.validate(df)
        ```

        :material-export: __output:__

        ``` markdown
        shape: (1, 12)
        ┌─────┬─────────────────────┬───────────────┬─────────┬────────┬─────────────┬───────┬──────┬────────────┬───────────┬────────────────┬────────┐
        │ id  ┆ timestamp           ┆ check         ┆ level   ┆ column ┆ rule        ┆ value ┆ rows ┆ violations ┆ pass_rate ┆ pass_threshold ┆ status │
        │ --- ┆ ---                 ┆ ---           ┆ ---     ┆ ---    ┆ ---         ┆ ---   ┆ ---  ┆ ---        ┆ ---       ┆ ---            ┆ ---    │
        │ i64 ┆ str                 ┆ str           ┆ str     ┆ str    ┆ str         ┆ str   ┆ i64  ┆ i64        ┆ f64       ┆ f64            ┆ str    │
        ╞═════╪═════════════════════╪═══════════════╪═════════╪════════╪═════════════╪═══════╪══════╪════════════╪═══════════╪════════════════╪════════╡
        │ 1   ┆ 2024-05-18 16:53:56 ┆ cuallee.check ┆ WARNING ┆ id     ┆ is_complete ┆ N/A   ┆ 5    ┆ 0          ┆ 1.0       ┆ 1.0            ┆ PASS   │
        └─────┴─────────────────────┴───────────────┴─────────┴────────┴─────────────┴───────┴──────┴────────────┴───────────┴────────────────┴────────┘

        ```

    === ":material-close-circle:{ .ko } FAIL"

        In this example, we intentionally place 2 `null` values in the dataframe and that produces a `FAIL` check as result.

        ``` py
        import polars as pl
        from cuallee import Check

        df = pl.DataFrame({"id" : [1,2,3,None, None]})
        check = Check()
        check.is_complete("id")

        # Validate
        check.validate(df)
        ```

        :material-export: __output:__

        ``` markdown
        shape: (1, 12)
        ┌─────┬─────────────────────┬───────────────┬─────────┬────────┬─────────────┬───────┬──────┬────────────┬───────────┬────────────────┬────────┐
        │ id  ┆ timestamp           ┆ check         ┆ level   ┆ column ┆ rule        ┆ value ┆ rows ┆ violations ┆ pass_rate ┆ pass_threshold ┆ status │
        │ --- ┆ ---                 ┆ ---           ┆ ---     ┆ ---    ┆ ---         ┆ ---   ┆ ---  ┆ ---        ┆ ---       ┆ ---            ┆ ---    │
        │ i64 ┆ str                 ┆ str           ┆ str     ┆ str    ┆ str         ┆ str   ┆ i64  ┆ i64        ┆ f64       ┆ f64            ┆ str    │
        ╞═════╪═════════════════════╪═══════════════╪═════════╪════════╪═════════════╪═══════╪══════╪════════════╪═══════════╪════════════════╪════════╡
        │ 1   ┆ 2024-05-18 16:53:56 ┆ cuallee.check ┆ WARNING ┆ id     ┆ is_complete ┆ N/A   ┆ 5    ┆ 2          ┆ 0.6       ┆ 1.0            ┆ FAIL   │
        └─────┴─────────────────────┴───────────────┴─────────┴────────┴─────────────┴───────┴──────┴────────────┴───────────┴────────────────┴────────┘
        ```



    === ":material-alert-circle:{ .kk } THRESHOLD"

        In this example, we validate reuse the data frame with empty values from the previous example, however we set our tolerance via the `pct` parameter on the rule `is_complete` to `0.6`. Producing now a `PASS` result on the check, regardless of the `2` present `null` values.

        ``` py
        import polars as pl
        from cuallee import Check

        df = pl.DataFrame({"id" : [1,2,3,None, None]})
        check = Check()
        check.is_complete("id", pct=0.6)

        # Validate
        check.validate(df)
        ```

        :material-export: __output:__

        ``` markdown
        shape: (1, 12)
        ┌─────┬─────────────────────┬───────────────┬─────────┬────────┬─────────────┬───────┬──────┬────────────┬───────────┬────────────────┬────────┐
        │ id  ┆ timestamp           ┆ check         ┆ level   ┆ column ┆ rule        ┆ value ┆ rows ┆ violations ┆ pass_rate ┆ pass_threshold ┆ status │
        │ --- ┆ ---                 ┆ ---           ┆ ---     ┆ ---    ┆ ---         ┆ ---   ┆ ---  ┆ ---        ┆ ---       ┆ ---            ┆ ---    │
        │ i64 ┆ str                 ┆ str           ┆ str     ┆ str    ┆ str         ┆ str   ┆ i64  ┆ i64        ┆ f64       ┆ f64            ┆ str    │
        ╞═════╪═════════════════════╪═══════════════╪═════════╪════════╪═════════════╪═══════╪══════╪════════════╪═══════════╪════════════════╪════════╡
        │ 1   ┆ 2024-05-18 16:53:56 ┆ cuallee.check ┆ WARNING ┆ id     ┆ is_complete ┆ N/A   ┆ 5    ┆ 2          ┆ 0.6       ┆ 0.6            ┆ PASS   │
        └─────┴─────────────────────┴───────────────┴─────────┴────────┴─────────────┴───────┴──────┴────────────┴───────────┴────────────────┴────────┘
        ```
