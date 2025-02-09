make inventory | grep cuallee | tr -d "'{}," | cut -d"." -f3- | tr -d " " | while IFS=: read -r engine_name test_name; do
    for engine in pyspark pandas duckdb bigquery polars snowpark; do
        mkdir -p "test/unit/engine/$engine/$engine_name/"
        touch "test/unit/engine/$engine/$engine_name/test_$test_name.py"
    done
done
