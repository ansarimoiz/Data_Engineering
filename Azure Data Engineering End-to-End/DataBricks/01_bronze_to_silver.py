# Databricks notebook source
# %pip install azure-storage-file-datalake azure-identity

# COMMAND ----------

# 3. Create a reusable ADLS connection

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

client_id = "8eaa3e68-f2ba-4270-a3da-0d4b8280d9ef"
client_secret = "tv98Q~RKbZua16Cf3s_fJ9JahQ1i.tTJ6TzTYarp"
tenant_id = "81447349-49a2-4f1d-b938-bd5f8aca3e21"

storage_account = "adlsproject03"
container = "nyctaxi"

credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret
)

service_client = DataLakeServiceClient(
    account_url=f"https://{storage_account}.dfs.core.windows.net",
    credential=credential
)

file_system_client = service_client.get_file_system_client(container)

# COMMAND ----------

#  4. Download the Taxi Zone lookup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
import tempfile

# COMMAND ----------

storage_account = "adlsproject03"
container = "nyctaxi"

bronze_base = "bronze/yellow"
silver_base = "silver/yellow"
zone_file = "reference/taxi_zones/taxi_zone_lookup.csv"

# COMMAND ----------

volume_path = "/Volumes/workspace/default/nyctaxi_volume"

# dbutils.fs.ls(volume_path)

# COMMAND ----------

# 6. Function to download an ADLS file to volume
def download_adls_to_volume(adls_path, volume_path):

    file_client = file_system_client.get_file_client(adls_path)

    download = file_client.download_file()

    data = download.readall()

    with open(volume_path, "wb") as f:
        f.write(data)

    return volume_path

# COMMAND ----------

bronze_file = (
    "bronze/yellow/year=2026/month=01/"
    "yellow_tripdata_2026-01.parquet"
)

local_file = (
    "/Volumes/workspace/default/nyctaxi_volume/"
    "bronze/yellow_tripdata_2026-01.parquet"
)

download_adls_to_volume(
    bronze_file,
    local_file
)

# COMMAND ----------

# Step 1 — Read the Bronze file

from pyspark.sql import functions as F

bronze_path = (
    "/Volumes/workspace/default/nyctaxi_volume/"
    "bronze/yellow_tripdata_2026-01.parquet"
)

df = spark.read.parquet(bronze_path)

print(f"Rows: {df.count():,}")
print(f"Columns: {len(df.columns)}")

display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Step 2 — Basic data-quality profile

df.select(
    F.count("*").alias("rows"),
    F.countDistinct(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID"
    ).alias("distinct_trip_combinations")
).display()

# COMMAND ----------

null_counts = df.select([
    F.count(
        F.when(F.col(c).isNull(), c)
    ).alias(c)
    for c in df.columns
])

display(null_counts)

# COMMAND ----------

# Step 3 — Convert timestamps

df = (
    df
    .withColumn(
        "tpep_pickup_datetime",
        F.to_timestamp("tpep_pickup_datetime")
    )
    .withColumn(
        "tpep_dropoff_datetime",
        F.to_timestamp("tpep_dropoff_datetime")
    )
)

df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
).display()

# Step 4 — Remove duplicate records
before = df.count()

df = df.dropDuplicates()

after = df.count()

print(f"Before duplicates removal: {before:,}")
print(f"After duplicates removal:  {after:,}")
print(f"Duplicates removed:       {before - after:,}")

# Step 5 — Calculate trip duration

df = df.withColumn(
    "trip_duration_minutes",
    (
        F.col("tpep_dropoff_datetime").cast("long")
        - F.col("tpep_pickup_datetime").cast("long")
    ) / 60
)

df.select(
    F.min("trip_duration_minutes").alias("min_duration"),
    F.max("trip_duration_minutes").alias("max_duration"),
    F.avg("trip_duration_minutes").alias("avg_duration")
).display()

# Step 6 — Data-quality filtering

df = df.filter(
    (F.col("trip_distance") >= 0) &
    (F.col("total_amount") >= 0) &
    (F.col("trip_duration_minutes") >= 0) &
    (F.col("trip_duration_minutes") <= 1440)
)

# Step 7 — Create analytical time columns

df = (
    df
    .withColumn(
        "pickup_date",
        F.to_date("tpep_pickup_datetime")
    )
    .withColumn(
        "pickup_year",
        F.year("tpep_pickup_datetime")
    )
    .withColumn(
        "pickup_month",
        F.month("tpep_pickup_datetime")
    )
    .withColumn(
        "pickup_day",
        F.dayofmonth("tpep_pickup_datetime")
    )
    .withColumn(
        "pickup_hour",
        F.hour("tpep_pickup_datetime")
    )
    .withColumn(
        "pickup_day_of_week",
        F.dayofweek("tpep_pickup_datetime")
    )
)

# COMMAND ----------

df = df.withColumn(
    "is_weekend",
    F.col("pickup_day_of_week").isin(1, 7)
)

# COMMAND ----------

# Step 8 — Add taxi-zone information
zone_adls_path = (
    "reference/taxi_zones/taxi_zone_lookup.parquet"
)

zone_local_file_path = (
    "/Volumes/workspace/default/nyctaxi_volume/"
    "reference/taxi_zone_lookup.parquet"
)

download_adls_to_volume(
    zone_adls_path,
    zone_local_file_path
)

zones = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .parquet(zone_local_file_path)
)
display(zones)


# COMMAND ----------



# COMMAND ----------



 



# Step 9 — Prepare pickup and dropoff dimensions

pickup_zones = zones.select(
    F.col("LocationID").alias("PULocationID"),
    F.col("Borough").alias("pickup_borough"),
    F.col("Zone").alias("pickup_zone"),
    F.col("service_zone").alias("pickup_service_zone")
)

dropoff_zones = zones.select(
    F.col("LocationID").alias("DOLocationID"),
    F.col("Borough").alias("dropoff_borough"),
    F.col("Zone").alias("dropoff_zone"),
    F.col("service_zone").alias("dropoff_service_zone")
)

# Step 10 — Join them
df = df.join(
    pickup_zones,
    on="PULocationID",
    how="left"
)

df = df.join(
    dropoff_zones,
    on="DOLocationID",
    how="left"
)

display(
    df.select(
        "PULocationID",
        "pickup_borough",
        "pickup_zone",
        "DOLocationID",
        "dropoff_borough",
        "dropoff_zone"
    ).limit(20)
)

# Step 11 — Add 2026 fee calculations
# Create a combined fee measure:
df = df.withColumn(
    "total_special_fees",
    F.coalesce(F.col("congestion_surcharge"), F.lit(0))
    + F.coalesce(F.col("Airport_fee"), F.lit(0))
    + F.coalesce(F.col("cbd_congestion_fee"), F.lit(0))
)
# And tip percentage:

df = df.withColumn(
    "tip_percentage",
    F.when(
        F.col("fare_amount") > 0,
        (F.col("tip_amount") / F.col("fare_amount")) * 100
    ).otherwise(F.lit(0))
)


# COMMAND ----------

# Step 12 — Create the Silver dataset
silver_df = df.select(
    "VendorID",

    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",

    "pickup_date",
    "pickup_year",
    "pickup_month",
    "pickup_day",
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend",

    "passenger_count",
    "trip_distance",
    "trip_duration_minutes",

    "RatecodeID",
    "store_and_fwd_flag",

    "PULocationID",
    "pickup_borough",
    "pickup_zone",
    "pickup_service_zone",

    "DOLocationID",
    "dropoff_borough",
    "dropoff_zone",
    "dropoff_service_zone",

    "payment_type",

    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",

    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",

    "total_special_fees",
    "tip_percentage"
)
silver_df.printSchema()
display(silver_df.limit(10))

# COMMAND ----------

# Step 13 — Write Silver to the Volume

silver_path = (
    "/Volumes/workspace/default/nyctaxi_volume/"
    "silver/yellow/"
)

(
    silver_df
    .write
    .mode("overwrite")
    .partitionBy("pickup_year", "pickup_month")
    .parquet(silver_path)
)

display(
    dbutils.fs.ls(silver_path)
)

# Step 14 — Validate Silver

silver_check = spark.read.parquet(silver_path)

print(f"Silver rows: {silver_check.count():,}")

display(silver_check.limit(10))

silver_check.groupBy(
    "pickup_year",
    "pickup_month"
).count().orderBy(
    "pickup_year",
    "pickup_month"
).display()

# COMMAND ----------

silver_df.groupBy(
    "pickup_year",
    "pickup_month"
).count().orderBy(
    "pickup_year",
    "pickup_month"
).show()

# COMMAND ----------

# Step 15 — Upload Silver back to ADLS

silver_clean = silver_df.drop(
    "pickup_year",
    "pickup_month",
    "pickup_day"
)

silver_clean = (
    silver_clean
    .withColumn("year", F.lit(2026))
    .withColumn("month", F.lit(1))
)

# COMMAND ----------

silver_clean_path = (
    "/Volumes/workspace/default/"
    "nyctaxi_volume/silver_clean/yellow"
)

(
    silver_clean
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(silver_clean_path)
)

# COMMAND ----------

import os

def upload_parquet_directory_to_adls(local_dir, adls_dir):

    for root, dirs, files in os.walk(local_dir):

        for file in files:

            # Ignore Spark metadata / commit files
            if not file.endswith(".parquet"):
                continue

            local_path = os.path.join(root, file)

            relative_path = os.path.relpath(
                local_path,
                local_dir
            ).replace("\\", "/")

            adls_path = f"{adls_dir}/{relative_path}"

            file_client = file_system_client.get_file_client(
                adls_path
            )

            with open(local_path, "rb") as data:
                file_client.upload_data(
                    data,
                    overwrite=True
                )

            print(f"Uploaded: {adls_path}")

# COMMAND ----------

upload_parquet_directory_to_adls(
    "/Volumes/workspace/default/nyctaxi_volume/silver_clean/yellow",
    "silver/yellow"
)