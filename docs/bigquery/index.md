# :simple-googlebigquery: BigQuery
In order to follow this examples, make sure your installation is all set for `bigquery`

!!! success "Install"
    ``` sh
    pip install cuallee
    pip install cuallee[bigquery]
    ```

## Pre-Requisites

You will need a Google Cloud account active, with the BigQuery API enabled to proceed with this examples.<br/>
Once your account is enabled with BigQuery, you will have to export a `service account` credential file in `json` format.

`cuallee` will read the environment variable `GOOGLE_APPLICATION_CREDENTIALS` expecting the name of the file that contains your `service account credentials`

!!! warning "Cost Associated"
    Be aware that running `cuallee` checks in `bigquery` incurs into cloud costs.


The process inside `cuallee` to handle the credentials is as follows:


!!! tip "Credentials Handling"
    ``` py
    import os
    from google.cloud import bigquery

    credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    client = bigquery.Client(project="GOOGLE_CLOUD_PROJECT_IDENTIFIER", credentials=credentials)

    ```


### is_complete

It validates the _completeness_ attribute of a data set. It confirms that a column does not contain `null` values   .


???+ example "is_complete"

    === ":material-checkbox-marked-circle:{ .ok } PASS"

        In this example, we validate that the column `id` does not have any missing values.

        ``` py
        from google.cloud import bigquery
        from cuallee import Check

        # Public dataset in BigQuery
        df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
        check = Check()
        check.is_complete("taxi_id")

        # Validate
        check.validate(df)
        ```

        :material-export: __output:__

        ``` markdown
                    timestamp          check    level   column         rule value       rows  violations  pass_rate  pass_threshold status
        id
        1   2024-05-18 21:24:15  cuallee.check  WARNING  taxi_id  is_complete   N/A  102589284           0        1.0             1.0   PASS
        ```

    === ":material-close-circle:{ .ko } FAIL"

        In this example, we intentionally place 2 `null` values in the dataframe and that produces a `FAIL` check as result.

        ``` py
        from google.cloud import bigquery
        from cuallee import Check

        # Public dataset in BigQuery
        df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
        check = Check()
        check.is_complete("trip_end_timestamp")

        # Validate
        check.validate(df)
        ```

        :material-export: __output:__

        ``` markdown
                    timestamp          check    level              column         rule value       rows  violations  pass_rate  pass_threshold status
        id
        1   2024-05-18 21:24:15  cuallee.check  WARNING  trip_end_timestamp  is_complete   N/A  102589284        1589   0.999985             1.0   FAIL
        ```



    === ":material-alert-circle:{ .kk } THRESHOLD"

        In this example, we validate reuse the data frame with empty values from the previous example, however we set our tolerance via the `pct` parameter on the rule `is_complete` to `0.6`. Producing now a `PASS` result on the check, regardless of the `2` present `null` values.

        ``` py
        from google.cloud import bigquery
        from cuallee import Check

        # Public dataset in BigQuery
        df = bigquery.dataset.Table("bigquery-public-data.chicago_taxi_trips.taxi_trips")
        check = Check()
        check.is_complete("trip_end_timestamp", pct=0.9)

        # Validate
        check.validate(df)
        ```

        :material-export: __output:__

        ``` markdown
                    timestamp          check    level              column         rule value       rows  violations  pass_rate  pass_threshold status
        id
        1   2024-05-18 21:24:15  cuallee.check  WARNING  trip_end_timestamp  is_complete   N/A  102589284        1589   0.999985             0.9   PASS
        ```
