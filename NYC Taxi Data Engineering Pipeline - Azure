# NYC Taxi Data Engineering Pipeline — Azure

An end-to-end data engineering pipeline built using **Azure Data Factory, Azure Data Lake Storage Gen2, Databricks, PySpark, Azure SQL Database, Synapse Serverless SQL, and Power BI**.

The project processes monthly NYC Yellow Taxi trip data using a **medallion architecture**, moving data from raw ingestion through transformation and dimensional modeling before exposing it through a serverless SQL layer for analytics.

---

## Architecture

flowchart LR

    A["NYC TLC<br/>Yellow Taxi Data"]

    B["Azure Data Factory<br/>Parameterized Incremental Ingestion"]

    C["Azure SQL Database<br/>Ingestion Control / Audit"]

    D["ADLS Gen2<br/>Bronze<br/>Raw Parquet"]

    E["Databricks + PySpark<br/>Clean / Standardize / Transform"]

    F["ADLS Gen2<br/>Silver<br/>Cleaned Parquet"]

    G["Databricks + PySpark<br/>Dimensional Modeling"]

    H["ADLS Gen2<br/>Gold<br/>Star Schema"]

    I["Synapse Serverless SQL<br/>OPENROWSET + Views"]

    J["Power BI<br/>Analytics"]

    A --> B
    C -. "Load status / incremental control" .-> B
    B --> D
    D --> E
    E --> F
    F --> G
    G --> H
    H --> I
    I --> J

    subgraph LAKE["Azure Data Lake Storage Gen2"]
        D
        F
        H
    end
```
---

## Project Overview

The goal of this project was to build a realistic cloud-based data engineering workflow around the **NYC TLC Yellow Taxi Trip Record Data**.

Instead of loading the data directly into a reporting database, the pipeline separates ingestion, transformation, storage, and serving layers.

### Pipeline

1. **NYC TLC** provides monthly Yellow Taxi trip data in Parquet format.
2. **Azure Data Factory** handles parameterized monthly ingestion.
3. Raw files are stored in the **Bronze** layer of ADLS Gen2.
4. **Databricks / PySpark** cleans and standardizes the data.
5. Cleaned data is written to the **Silver** layer.
6. PySpark creates a dimensional **Gold star schema**.
7. Gold datasets are stored as Parquet in ADLS Gen2.
8. **Synapse Serverless SQL** exposes the Parquet data through SQL views.
9. **Power BI** connects to the Synapse SQL layer for reporting and analysis.

---

## Technology Stack

| Layer | Technology |
|---|---|
| Source | NYC TLC Yellow Taxi Trip Records |
| Orchestration | Azure Data Factory |
| Data Lake | Azure Data Lake Storage Gen2 |
| Processing | Databricks |
| Transformation | PySpark |
| Control / Audit | Azure SQL Database |
| Serving | Azure Synapse Serverless SQL |
| Visualization | Power BI |
| Storage Format | Parquet |
| Compression | Snappy |
| Programming | Python / SQL / PySpark |

---

## Medallion Architecture

### Bronze

The Bronze layer contains the raw monthly source files received from the NYC TLC dataset.

Example:

```text
bronze/
└── yellow/
    └── year=2026/
        ├── month=1/
        │   └── yellow_tripdata_2026-01.parquet
        └── month=2/
            └── yellow_tripdata_2026-02.parquet
```

The Bronze layer preserves the source data before transformation.

---

### Silver

The Silver layer contains cleaned and standardized data produced using PySpark.

Transformations include:

- Data type standardization
- Null handling
- Column cleanup
- Trip duration calculation
- Data quality filtering
- Derived analytical fields
- Source-month partitioning

The actual pickup and drop-off timestamps are retained for analysis.

Example:

```text
silver/
└── yellow/
    └── year=2026/
        └── month=1/
            ├── part-00000-....snappy.parquet
            ├── part-00001-....snappy.parquet
            └── ...
```

The Silver data is partitioned using the **source processing month**, rather than deriving storage partitions from individual trip timestamps.

This keeps incremental processing aligned with the monthly source files.

---

## Gold Layer

The Gold layer converts the cleaned trip data into a **star schema** designed for analytics.

### Fact Table

#### `fact_taxi_trips`

Contains the individual taxi trip records and analytical measures.

Key fields include:

```text
trip_id
tpep_pickup_datetime
tpep_dropoff_datetime
PULocationID
DOLocationID
payment_type
RatecodeID
VendorID
passenger_count
trip_distance
trip_duration_minutes
fare_amount
tip_amount
tolls_amount
total_amount
congestion_surcharge
Airport_fee
cbd_congestion_fee
total_special_fees
tip_percentage
year
month
```

A deterministic `trip_id` is generated using a SHA-256 hash based on selected trip attributes.

---

### Dimension Tables

The Gold layer contains the following dimensions:

```text
dim_date
dim_zone
dim_payment
dim_rate_code
```

#### `dim_date`

Provides calendar attributes for analytical reporting.

#### `dim_zone`

Contains NYC taxi zone information including:

- Location ID
- Borough
- Zone
- Service zone

#### `dim_payment`

Provides payment type descriptions.

#### `dim_rate_code`

Provides rate code classifications.

---

## Gold Storage Structure

```text
gold/
├── dim_date/
├── dim_payment/
├── dim_rate_code/
├── dim_zone/
└── fact_taxi_trips/
    └── year=2026/
        └── month=1/
```

The Gold layer remains stored as Parquet in ADLS rather than being copied into a separate physical warehouse table.

---

## Incremental Processing

The pipeline is designed around **monthly incremental ingestion**.

The source dataset is naturally published by month, so the pipeline uses the source year/month as the processing boundary.

An Azure SQL control table tracks ingestion status.

Conceptually:

```text
Azure Data Factory
        │
        ▼
ingestion_log
        │
        ├── year
        ├── month
        ├── status
        └── ingestion metadata
```

This allows the pipeline to determine which source periods have already been processed instead of treating every execution as a full reload.

---

## Why Parquet?

Parquet was used throughout the data lake because it provides:

- Columnar storage
- Efficient analytical reads
- Compression
- Schema information
- Compatibility with Spark and Synapse Serverless SQL

The Gold layer can therefore be queried directly from ADLS without first loading the entire dataset into a traditional database.

---

## Synapse Serverless SQL

Azure Synapse Serverless SQL acts as the serving layer.

The Gold Parquet files are queried directly from ADLS using `OPENROWSET`.

Views were created to provide a cleaner SQL interface:

```text
gold.v_fact_taxi_trips
gold.v_dim_date
gold.v_dim_zone
gold.v_dim_payment
gold.v_dim_rate_code
```

This separates the physical Parquet storage from the analytical SQL interface consumed by Power BI.

---

## Data Validation

The pipeline was validated by comparing the Databricks Gold outputs with the Synapse Serverless SQL views.

Current January 2026 validation:

| Dataset | Records |
|---|---:|
| Fact Taxi Trips | 3,684,871 |
| Date Dimension | 33 |
| Zone Dimension | 265 |
| Payment Dimension | 5 |
| Rate Code Dimension | 7 |

The fact count matched between the Databricks transformation layer and Synapse SQL serving layer.

---

## Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

- Cloud data lake architecture
- Medallion architecture
- ETL / ELT pipelines
- Incremental ingestion
- Parameterized Azure Data Factory pipelines
- ADLS Gen2
- PySpark transformations
- Parquet data processing
- Data quality handling
- Dimensional modeling
- Star schemas
- Fact and dimension tables
- Serverless SQL
- SQL views over data lake files
- Data validation
- Analytical data modeling
- Power BI integration

---

## Repository Structure

A suggested repository structure:

```text
nyc-taxi-azure-data-engineering/
│
├── adf/
│   ├── pipelines/
│   ├── datasets/
│   └── linked-services/
│
├── databricks/
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── utilities/
│
├── synapse/
│   ├── external_data_source.sql
│   └── gold_views.sql
│
├── sql/
│   └── ingestion_log.sql
│
├── powerbi/
│   └── screenshots/
│
├── docs/
│   └── architecture.mmd
│
└── README.md
```

---

## Project Workflow

```text
Source
  │
  ▼
NYC TLC Monthly Parquet
  │
  ▼
Azure Data Factory
  │
  ├──────────────► Azure SQL
  │                 Control / Audit
  │
  ▼
ADLS Bronze
  │
  ▼
Databricks / PySpark
  │
  ▼
ADLS Silver
  │
  ▼
Databricks / PySpark
  │
  ▼
Gold Star Schema
  │
  ├── fact_taxi_trips
  ├── dim_date
  ├── dim_zone
  ├── dim_payment
  └── dim_rate_code
  │
  ▼
ADLS Gold
  │
  ▼
Synapse Serverless SQL
  │
  ▼
Power BI
```

---

## Key Design Decisions

### Source-month partitioning

The pipeline uses the source file's year/month as the storage partition boundary.

This avoids unnecessarily deriving storage partitions from the actual trip timestamp while still retaining the original pickup and drop-off timestamps for analytics.

### Data lake as the primary storage layer

Instead of moving every transformation into SQL tables, the project keeps Bronze, Silver, and Gold data in ADLS using Parquet.

This provides a clear separation between storage, transformation, and serving.

### Serverless serving layer

Synapse Serverless SQL provides a SQL interface over the Gold Parquet files without requiring a dedicated data warehouse for the project.

### Star schema

The Gold layer separates trip-level facts from reusable dimensions, making the resulting dataset easier to consume from BI tools.

---

## Data Source

NYC Taxi & Limousine Commission — Trip Record Data

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page?utm_source=chatgpt.com)

The project uses the publicly available NYC Yellow Taxi trip record data for the pipeline demonstration.
