# 🚗 NYC Taxi 2026 Data Engineering End-to-End Project

## Objective

In this project, I designed and implemented an end-to-end data pipeline that consists of several stages:
1. Extracted data from NYC Trip Record Data website and loaded into Google Cloud Storage for further processing.
3. Transformed and modeled the data using fact and dimensional data modeling concepts using Python on Google Colab.
4. Using ETL concept, I orchestrated the data pipeline on Mage AI and loaded the transformed data into Google BigQuery.
5. Developed a dashboard on Looker Studio.

As this is a data engineering project, my emphasis is primarily on the engineering aspect with a lesser emphasis on analytics and dashboard development.

The sections below will explain additional details on the technologies and files utilized.

## Table of Content

- [Dataset Used](#dataset-used)
- [Technologies](technologies)
- [Data Pipeline Architecture](#data-pipeline-architecture)
- [Date Modeling](#data-modeling)
- [Step 1: Cleaning and Transformation](#step-1-cleaning-and-transformation)
- [Step 2: Storage](#step-2-storage)
- [Step 3: ETL / Orchestration](#step-3-etl--orchestration)
- [Step 4: Analytics](#step-4-analytics)
- [Step 5: Dashboard](#step-5-dashboard)

## Dataset Used

This project uses the NYC Taxi Trip Record Data which include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.

More info about dataset can be found in the following links:
- Website: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Data Dictionary: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
<!-- - Raw Data (CSV):  https://github.com/ansarimoiz/Data_Engineering/blob/main/Uber%20Project/uber_data.csv  -->

## Technologies

The following technologies are used to build this project:
- Language: Python, SQL
- Extraction and transformation: Google Colab, Google BigQuery
- Storage: Google Cloud Storage
- Orchestration: [Mage AI](https://www.mage.ai)
- Dashboard: [Looker Studio](https://lookerstudio.google.com)

## Data Pipeline Architecture

<img width="897" alt="Screenshot 2023-05-08 at 11 49 09 AM" src="https://user-images.githubusercontent.com/81607668/236729698-65e193bc-75ee-4ea6-9040-f33f5f2958cb.png">

Files in the following stages:
- Step 1: Cleaning and transformation - [Data Engineering.ipynb](https://colab.research.google.com/drive/1Y6l2igEKF4lwClUYKAJlfGTjbfdAE_Db?usp=sharing)
- Step 2: Storage
- Step 3: ETL, Orchestration - Mage: [Extract](https://github.com/ansarimoiz/Data_Engineering/blob/25f461d67ab55fb2c429a99c19483ad82ead6304/Uber%20Project/Mage/extract_data.py), [Transform](https://github.com/ansarimoiz/Data_Engineering/blob/25f461d67ab55fb2c429a99c19483ad82ead6304/Uber%20Project/Mage/transform_data.py), [Load](https://github.com/ansarimoiz/Data_Engineering/blob/25f461d67ab55fb2c429a99c19483ad82ead6304/Uber%20Project/Mage/load_data.py)
- Step 4: Analytics - [SQL script](https://github.com/ansarimoiz/Data_Engineering/blob/3a5e0ee52c5497bbe69782e7d72840ae06416683/Uber%20Project/sqL_script.sql)
- Step 5: [Dashboard](https://datastudio.google.com/reporting/8e58e478-86ff-4a5a-b824-6c88dc6d1918)

## Data Modeling

The datasets are designed using the principles of fact and dim data modeling concepts. 

![Data Model](https://user-images.githubusercontent.com/81607668/236725688-995b6049-26c1-440f-b523-7c6c10d507ba.png)

## Step 1: Cleaning and Transformation

In this step, I loaded the CSV file into Google Colab and carried out data cleaning and transformation activities prior to organizing them into fact and dim tables.

<!--  Here's the specific cleaning and transformation tasks that were performed:
1. Converted `tpep_pickup_datetime` and `tpep_dropoff_datetime` columns into datetime format.
2. Removed duplicates and reset the index.-->

Link to the script: [Data Engineering](https://colab.research.google.com/drive/1Y6l2igEKF4lwClUYKAJlfGTjbfdAE_Db?usp=sharing)

<img width="1436" alt="image" src="https://github.com/ansarimoiz/Data_Engineering/blob/3e45722788fca6823c31033a46114e5c045bc305/Uber%20Project/Assets/data_modelling.png">

After completing the above steps, I created the fact and dimension tables.

<img width="1436" alt="image" src="https://github.com/ansarimoiz/Data_Engineering/blob/3e45722788fca6823c31033a46114e5c045bc305/Uber%20Project/Assets/fact_table.png">

## Step 2: Storage

<img width="1436" alt="image" src="https://github.com/ansarimoiz/Data_Engineering/blob/3e45722788fca6823c31033a46114e5c045bc305/Uber%20Project/Assets/cloud_storage.png">

## Step 3: ETL / Orchestration

1. Begin by launching the SSH instance and running the following commands below to install the required libraries.

```python
# Install python and pip 
# Update Ubuntu packages
sudo apt update
 
# Install Python, pip, and tools
sudo apt install -y python3 python3-pip python3-venv

# Create a project environment
python3 -m venv uber-env

# Activate it
source uber-env/bin/activate

# Upgrade pip  (update pip to latest version)
python -m pip install --upgrade pip  

# Install Python libraries  
pip install --upgrade pandas  
pip install --upgrade google-cloud-bigquery
pip install --upgrade google-cloud-storage
```

2. After that, I install the Mage AI library from the [Mage AI GitHub](https://github.com/mage-ai/mage-ai#using-pip-or-conda). Then, I create a new project called "nyc_taxi_project".

```python 
# Install Mage library
pip install mage-ai

# Create new project
mage start demo_project
```

3. Next, I conduct orchestration in Mage by accessing the external IP address through a new tab. The link format is: `<external IP address>:<port number>`.

After that, I create a new pipeline with the following stages:
- Extract: [load_uber_data](https://github.com/ansarimoiz/Data_Engineering/blob/25f461d67ab55fb2c429a99c19483ad82ead6304/Uber%20Project/Mage/extract_data.py)  
- Transform: [transform_uber](https://github.com/ansarimoiz/Data_Engineering/blob/25f461d67ab55fb2c429a99c19483ad82ead6304/Uber%20Project/Mage/transform_data.py)
- Load: [load_gbq](https://github.com/ansarimoiz/Data_Engineering/blob/25f461d67ab55fb2c429a99c19483ad82ead6304/Uber%20Project/Mage/load_data.py)

<img width="1438" alt="image" src="https://github.com/katiehuangx/data-engineering/assets/81607668/ae8acb39-c66e-41f6-b81b-d1179121c0a4">

Before executing the Load pipeline, I download credentials from Google API & Credentials and then update them accordingly in the `io_config.yaml` file within the same pipeline. This step is essential for granting authorization to access and load data into Google BigQuery.

## Step 4: Analytics

After running the Load pipeline in Mage, the fact and dim tables are generated in Google BigQuery.

<img width="1438" alt="Screenshot" src="https://github.com/ansarimoiz/Data_Engineering/blob/3e45722788fca6823c31033a46114e5c045bc305/Uber%20Project/Assets/big_query.png">

<!-- 
Here's the additional analyses I performed:
1. Find the top 10 pickup locations based on the number of trips
<img width="1436" alt="Screenshot 2023-09-03 at 3 46 17 PM" src="https://github.com/katiehuangx/data-engineering/assets/81607668/87fef0c1-f849-4b0e-8f2d-db68a989a06d">

2. Find the total number of trips by passenger count:
<img width="1436" alt="Screenshot 2023-09-03 at 3 47 48 PM" src="https://github.com/katiehuangx/data-engineering/assets/81607668/5f563142-9d18-4019-8499-7d9958b7ec05">

3. Find the average fare amount by hour of the day:
<img width="1436" alt="Screenshot 2023-09-03 at 3 48 52 PM" src="https://github.com/katiehuangx/data-engineering/assets/81607668/bf8d4dea-0915-48fb-a673-e5b3d3f37e3f">
-->

## Step 5: Dashboard

After completing the analysis, I loaded the relevant tables into Looker Studio and created a dashboard, which you can view [here](https://datastudio.google.com/reporting/8e58e478-86ff-4a5a-b824-6c88dc6d1918).

![Dashoard Pg 1](https://github.com/ansarimoiz/Data_Engineering/blob/3e45722788fca6823c31033a46114e5c045bc305/Uber%20Project/Assets/dashboard_1.png)

![Dashboard Pg 2](https://github.com/ansarimoiz/Data_Engineering/blob/3e45722788fca6823c31033a46114e5c045bc305/Uber%20Project/Assets/dashboard_2.png)

***
