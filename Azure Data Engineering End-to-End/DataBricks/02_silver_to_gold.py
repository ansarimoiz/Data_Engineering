# Databricks notebook source
from pyspark.sql import functions as F

volume_base = "/Volumes/workspace/default/nyctaxi_volume"

silver_path = f"{volume_base}/silver_clean/yellow"

silver_df = spark.read.parquet(silver_path)

print("Rows:", silver_df.count())
silver_df.printSchema()

# COMMAND ----------

## Creating the fact table

fact_df = silver_df.withColumn(
    "trip_id",
    F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("VendorID").cast("string"), F.lit("")),
            F.coalesce(F.col("tpep_pickup_datetime").cast("string"), F.lit("")),
            F.coalesce(F.col("tpep_dropoff_datetime").cast("string"), F.lit("")),
            F.coalesce(F.col("PULocationID").cast("string"), F.lit("")),
            F.coalesce(F.col("DOLocationID").cast("string"), F.lit("")),
            F.coalesce(F.col("total_amount").cast("string"), F.lit(""))
        ),
        256
    )
)

fact_taxi_trips = fact_df.select(
    "trip_id",

    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",

    "PULocationID",
    "DOLocationID",

    "payment_type",
    "RatecodeID",
    "VendorID",

    "passenger_count",
    "trip_distance",
    "trip_duration_minutes",

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


## Creating dim_date
dim_date = (
    silver_df
    .select(
        F.to_date("tpep_pickup_datetime").alias("date")
    )
    .where(F.col("date").isNotNull())
    .distinct()
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_week", F.dayofweek("date"))
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn(
        "is_weekend",
        F.col("day_of_week").isin(1, 7)
    )
)

# COMMAND ----------

display(dim_date.orderBy("date"))

# COMMAND ----------

zone_path = (
    f"{volume_base}/reference/taxi_zone_lookup.parquet"
)

zones = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .parquet(zone_path)
)

dim_zone = (
    zones
    .select(
        F.col("LocationID").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone")
    )
    .dropDuplicates(["location_id"])
)

# COMMAND ----------

#5. Create dim_payment
dim_payment = (
    silver_df
    .select("payment_type")
    .where(F.col("payment_type").isNotNull())
    .distinct()
)

dim_payment = dim_payment.withColumn(
    "payment_method",
    F.when(F.col("payment_type") == 0, "Unknown")
     .when(F.col("payment_type") == 1, "Credit card")
     .when(F.col("payment_type") == 2, "Cash")
     .when(F.col("payment_type") == 3, "No charge")
     .when(F.col("payment_type") == 4, "Dispute")
     .when(F.col("payment_type") == 5, "Unknown")
     .when(F.col("payment_type") == 6, "Voided trip")
     .otherwise("Other")
)

#6. Create dim_rate_code
dim_rate_code = (
    silver_df
    .select("RatecodeID")
    .where(F.col("RatecodeID").isNotNull())
    .distinct()
)

dim_rate_code = dim_rate_code.withColumn(
    "rate_code_description",
    F.when(F.col("RatecodeID") == 1, "Standard rate")
     .when(F.col("RatecodeID") == 2, "JFK")
     .when(F.col("RatecodeID") == 3, "Newark")
     .when(F.col("RatecodeID") == 4, "Nassau or Westchester")
     .when(F.col("RatecodeID") == 5, "Negotiated fare")
     .when(F.col("RatecodeID") == 6, "Group ride")
     .otherwise("Other")
)

fact_gold = (
    fact_taxi_trips
    .withColumn("year", F.lit(2026))
    .withColumn("month", F.lit(1))
)

# 7. Write Gold to the Databricks Volume
gold_path = f"{volume_base}/gold"

(
    fact_gold
    .write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(
        f"{gold_path}/fact_taxi_trips"
    )
)

# COMMAND ----------

## Writing dimensions to Volume

dim_date.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/dim_date")

dim_zone.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/dim_zone")

dim_payment.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/dim_payment")

dim_rate_code.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/dim_rate_code")

# COMMAND ----------

#8. Validate Gold

print("Fact:", fact_gold.count())
print("Date:", dim_date.count())
print("Zone:", dim_zone.count())
print("Payment:", dim_payment.count())
print("Rate:", dim_rate_code.count())
display(fact_gold.limit(10))
display(dim_zone.limit(10))


# COMMAND ----------

fact_gold.groupBy("year", "month").count().show()

# COMMAND ----------

#   Create a reusable ADLS connection

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

#   Function to download an ADLS file to volume
def download_adls_to_volume(adls_path, volume_path):

    file_client = file_system_client.get_file_client(adls_path)

    download = file_client.download_file()

    data = download.readall()

    with open(volume_path, "wb") as f:
        f.write(data)

    return volume_path

# COMMAND ----------

## Uploading Gold to ADLS

import os

def upload_parquet_directory_to_adls(local_dir, adls_dir):

    uploaded = 0

    for root, dirs, files in os.walk(local_dir):

        for file in files:

            # Only upload actual Parquet data files
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

            uploaded += 1
            print(f"Uploaded: {adls_path}")

    print(f"\nTotal Parquet files uploaded: {uploaded}")

# COMMAND ----------

gold_volume_path = (
    "/Volumes/workspace/default/"
    "nyctaxi_volume/gold"
)

upload_parquet_directory_to_adls(
    gold_volume_path,
    "gold"
)

# COMMAND ----------

paths = file_system_client.get_paths(
    path="gold"
)

for path in paths:
    if path.name.endswith(".parquet"):
        print(path.name)