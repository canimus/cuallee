# :simple-snowflake: Snowpark
In order to follow this examples, make sure your installation is all set for `snowpark`

!!! success "Install"
    ``` sh
    pip install cuallee
    pip install cuallee[snowpark]
    ```

## Pre-Requisites
You will have a SnowFlake account in order to proceed with the examples below.<br/>
Once you get an account, you can obtain your account details normally located in the bottom left corner of your SnowFlake environment.</br>
The following environment variables are required and used during runtime from `cuallee` to connect to your instance:

- `SF_ACCOUNT`=`hp00000.us-east4.gcp`
- `SF_USER`=`user.name@cuallee.com`
- `SF_PASSWORD`=`MySecretPa$$word?`
- `SF_ROLE`=`ACCOUNTADMIN`
- `SF_WAREHOUSE`=`COMPUTE_WH`
- `SF_DATABASE`=`SNOWFLAKE_SAMPLE_DATA`
- `SF_SCHEMA`=`TPCH_SF10`


!!! warning "Cost Associated"
    Be aware that running `cuallee` checks in `snowpark` incurs into cloud costs.


## In SnowFlake

The following __How-To__ guide, explains the steps to configure `cuallee` directly in SnowFlake:

[SnowFlake Data Quality Setup](https://github.com/canimus/cuallee/wiki/Snowflake)



### is_complete

It validates the _completeness_ attribute of a data set. It confirms that a column does not contain `null` values.


???+ example "is_complete"

    === ":material-checkbox-marked-circle:{ .ok } PASS"

        In this example, we validate that the column `id` does not have any missing values.

        ``` py
        import os
        from snowflake.snowpark import Session
        from cuallee import Check

        settings = {
            "account": os.getenv("SF_ACCOUNT"),
            "user": os.getenv("SF_USER"),
            "password": os.getenv("SF_PASSWORD"),
            "role": os.getenv("SF_ROLE"),
            "warehouse": os.getenv("SF_WAREHOUSE"),
            "database": os.getenv("SF_DATABASE"),
            "schema": os.getenv("SF_SCHEMA"),
        }

        snowpark = Session.builder.configs(settings).create()
        df = snowpark.range(10)
        check = Check()
        check.is_complete("ID")

        # Validate
        check.validate(df).show()
        ```

        :material-export: __output:__

        ``` markdown
        ---------------------------------------------------------------------------------------------------------------------------------------------------------------
        |"ID"  |"TIMESTAMP"          |"CHECK"        |"LEVEL"  |"COLUMN"  |"RULE"       |"VALUE"  |"ROWS"  |"VIOLATIONS"  |"PASS_RATE"  |"PASS_THRESHOLD"  |"STATUS"  |
        ---------------------------------------------------------------------------------------------------------------------------------------------------------------
        |1     |2024-05-18 20:47:28  |cuallee.check  |WARNING  |ID        |is_complete  |N/A      |10      |0.0           |1.0          |1.0               |PASS      |
        ---------------------------------------------------------------------------------------------------------------------------------------------------------------
        ```

    === ":material-close-circle:{ .ko } FAIL"

        In this example, we intentionally place 2 `null` values in the dataframe and that produces a `FAIL` check as result.

        ``` py
        import os
        from snowflake.snowpark import Session
        from cuallee import Check

        settings = {
            "account": os.getenv("SF_ACCOUNT"),
            "user": os.getenv("SF_USER"),
            "password": os.getenv("SF_PASSWORD"),
            "role": os.getenv("SF_ROLE"),
            "warehouse": os.getenv("SF_WAREHOUSE"),
            "database": os.getenv("SF_DATABASE"),
            "schema": os.getenv("SF_SCHEMA"),
        }

        snowpark = Session.builder.configs(settings).create()
        df = snowpark.range(8).union_all(snowpark.create_dataframe([[None], [None]], ["ID"]))
        check = Check()
        check.is_complete("ID")

        # Validate
        check.validate(df).show()
        ```

        :material-export: __output:__

        ``` markdown
        ---------------------------------------------------------------------------------------------------------------------------------------------------------------
        |"ID"  |"TIMESTAMP"          |"CHECK"        |"LEVEL"  |"COLUMN"  |"RULE"       |"VALUE"  |"ROWS"  |"VIOLATIONS"  |"PASS_RATE"  |"PASS_THRESHOLD"  |"STATUS"  |
        ---------------------------------------------------------------------------------------------------------------------------------------------------------------
        |1     |2024-05-18 20:47:28  |cuallee.check  |WARNING  |ID        |is_complete  |N/A      |10      |2.0           |0.8          |1.0               |FAIL      |
        ---------------------------------------------------------------------------------------------------------------------------------------------------------------
        ```



    === ":material-alert-circle:{ .kk } THRESHOLD"

        In this example, we validate reuse the data frame with empty values from the previous example, however we set our tolerance via the `pct` parameter on the rule `is_complete` to `0.8`. Producing now a `PASS` result on the check, regardless of the `2` present `null` values.

        ``` py
        import os
        from snowflake.snowpark import Session
        from cuallee import Check

        settings = {
            "account": os.getenv("SF_ACCOUNT"),
            "user": os.getenv("SF_USER"),
            "password": os.getenv("SF_PASSWORD"),
            "role": os.getenv("SF_ROLE"),
            "warehouse": os.getenv("SF_WAREHOUSE"),
            "database": os.getenv("SF_DATABASE"),
            "schema": os.getenv("SF_SCHEMA"),
        }

        snowpark = Session.builder.configs(settings).create()
        df = snowpark.range(8).union_all(snowpark.create_dataframe([[None], [None]], ["ID"]))
        check = Check()
        check.is_complete("ID", pct=0.8)

        # Validate
        check.validate(df).show()
        ```

        :material-export: __output:__

        ``` markdown
        ---------------------------------------------------------------------------------------------------------------------------------------------------------------
        |"ID"  |"TIMESTAMP"          |"CHECK"        |"LEVEL"  |"COLUMN"  |"RULE"       |"VALUE"  |"ROWS"  |"VIOLATIONS"  |"PASS_RATE"  |"PASS_THRESHOLD"  |"STATUS"  |
        ---------------------------------------------------------------------------------------------------------------------------------------------------------------
        |1     |2024-05-18 20:47:28  |cuallee.check  |WARNING  |ID        |is_complete  |N/A      |10      |2.0           |0.8          |0.8               |PASS      |
        ---------------------------------------------------------------------------------------------------------------------------------------------------------------
        ```
