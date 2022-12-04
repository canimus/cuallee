# import Scan from Soda Core
from soda.scan import Scan
from pyspark.sql import SparkSession
from cuallee.pyspark_validation import numeric_fields, timestamp_fields

spark = SparkSession.builder.config("spark.driver.memory", "16g").getOrCreate()


# Create a Spark DataFrame, or use the Spark API to read data and create a DataFrame
df = spark.read.parquet("data/*.parquet")


# Create a view that SodaCL uses as a dataset
df.createOrReplaceTempView("my_df")
# Create a Scan object, set a scan definition, and attach a Spark session
scan = Scan()
scan.set_scan_definition_name("test")
scan.set_data_source_name("spark_df")
scan.add_spark_session(spark)
# Define checks for datasets 
checks  ="""
checks for my_df:
    - missing_count(VendorID) = 0
    - missing_count(tpep_pickup_datetime) = 0
    - missing_count(tpep_dropoff_datetime) = 0
    - missing_count(passenger_count) = 0
    - missing_count(trip_distance) = 0
    - missing_count(RatecodeID) = 0
    - missing_count(store_and_fwd_flag) = 0
    - missing_count(PULocationID) = 0
    - missing_count(DOLocationID) = 0
    - missing_count(payment_type) = 0
    - missing_count(fare_amount) = 0
    - missing_count(extra) = 0
    - missing_count(mta_tax) = 0
    - missing_count(tip_amount) = 0
    - missing_count(tolls_amount) = 0
    - missing_count(improvement_surcharge) = 0
    - missing_count(total_amount) = 0
    - missing_count(congestion_surcharge) = 0
    - missing_count(airport_fee) = 0
    - missing_count(passenger_count) = 0
    - missing_count(improvement_surcharge) = 0
    - missing_count(airport_fee) = 0
    - missing_count(DOLocationID) = 0
    - missing_count(total_amount) = 0
    - missing_count(tip_amount) = 0
    - missing_count(trip_distance) = 0
    - missing_count(payment_type) = 0
    - duplicate_count(VendorID) = 0
    - duplicate_count(tpep_pickup_datetime) = 0
    - duplicate_count(tpep_dropoff_datetime) = 0
    - duplicate_count(passenger_count) = 0
    - duplicate_count(trip_distance) = 0
    - duplicate_count(RatecodeID) = 0
    - duplicate_count(store_and_fwd_flag) = 0
    - duplicate_count(PULocationID) = 0
    - duplicate_count(DOLocationID) = 0
    - duplicate_count(payment_type) = 0
    - duplicate_count(fare_amount) = 0
    - duplicate_count(extra) = 0
    - duplicate_count(mta_tax) = 0
    - duplicate_count(tip_amount) = 0
    - duplicate_count(tolls_amount) = 0
    - duplicate_count(improvement_surcharge) = 0
    - duplicate_count(total_amount) = 0
    - duplicate_count(congestion_surcharge) = 0
    - duplicate_count(airport_fee) = 0
    - duplicate_count(passenger_count) = 0
    - duplicate_count(improvement_surcharge) = 0
    - duplicate_count(airport_fee) = 0
    - duplicate_count(DOLocationID) = 0
    - duplicate_count(total_amount) = 0
    - duplicate_count(tip_amount) = 0
    - duplicate_count(trip_distance) = 0
    - duplicate_count(payment_type) = 0
"""
# If you defined checks in a file accessible via Spark, you can use the scan.add_sodacl_yaml_file method to retrieve the checks
scan.add_sodacl_yaml_str(checks)
# Optionally, add a configuration file with Soda Cloud credentials 
# config = """
# soda_cloud:
#   api_key_id: xyz
#   api_key_secret: xyz
# """
# scan.add_configuration_yaml_str(config)

# Execute a scan
scan.execute()
# Check the Scan object for methods to inspect the scan result; the following prints all logs to console
print(scan.get_logs_text())