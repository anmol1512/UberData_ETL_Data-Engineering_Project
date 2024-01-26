# UberData ETL Data Engineering Project

## Overview
This repository contains the source code and configurations for an `ETL (Extract, Transform, Load)` data engineering project focused on Uber data. The project encompasses various components, including an Analytics layer, data pipeline tree, Mage UI, Mage pipeline blocks, and more.

## Architecture
[![architecture.jpg](https://i.postimg.cc/XYxgS47v/architecture.jpg)](https://postimg.cc/XrG59M5M)

## Table of Contents
1. [Dashboard](#dashboard)
2. [Data Modeling](#data-modeling)
3. [ETL](#etl)
4. [Data Pipeline](#data-pipeline)
5. [Analytics Layer](#analytics-layer)
6. [Dataset Used](#dataset)
7. [Config Files](#config-files)
8. [Requirements](#requirements)

## Dashboard

![Descriptive-Analytics](https://i.postimg.cc/J0vdVm1g/Descriptive-Analytics-DASHBOARD.png)

![Map-Visualization](https://i.postimg.cc/3rcqDjgB/Visualization-Analysis-DASHBOARD.png)

## Data Modeling

![Data-Model-Uber-Project.jpg](https://i.postimg.cc/gjhRBPrN/Data-Model-Uber-Project.jpg)

## ETL
`data_exporter.py`

This module is responsible for exporting data to a BigQuery warehouse. It utilizes Mage AI's `BigQuery` class and reads configurations from `io_config.yaml`.

```python
# Example usage:
# export_data_to_big_query(df, table_name='example_table')

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

# ... (imports and setup)

@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    # Implementation details...
```

`data_loader.py`

This module demonstrates loading data from an API using the `requests` library. The data is fetched from a specified URL and read into a Pandas DataFrame.

```python
# Example usage:
# df = load_data_from_api()

import io
import pandas as pd
import requests

# ... (imports)

@data_loader
def load_data_from_api(*args, **kwargs):
    # Implementation details...
```

`transformer.py`

This module provides a template for a transformer block, showcasing data transformation operations using Pandas. The example includes handling duplicates, dropping rows with missing values, converting datetime columns, generating a new trip_id column, and creating dimension tables.

```python
# Example usage:
# output = transform(input_df)

import pandas as pd

# ... (imports)

@transformer
def transform(df, *args, **kwargs):
    # Transformation logic...

    return output_tables

@test
def test_output(output, *args) -> None:
    # Testing logic...
```

## Data Pipeline

![Tree-for-data-pipeline](https://i.postimg.cc/QC8W8WMK/Tree-or-DAG-structure-Uber-data-pipeline.png)

## Analytics Layer
The Analytics Layer in this project contains essential components for data analysis and querying. Below are the details of the files included in this layer:

### Job Execution Graph
- File: `Job_Execution_Graph.png`

![Job-Execution-Graph.png](https://i.postimg.cc/y8PxNG0W/Job-Execution-Graph.png)

This visual representation provides insights into the execution flow of jobs within the analytics layer.

### BigQuery Analytics Query
- File: `bigquery_analytics.sql`

The `bigquery_analytics.sql` file contains the SQL query used to create a table (`bigquery_analytics_query`) within the Uber_dataset in BigQuery. This table is a result of joining various dimensions and fact tables to form a comprehensive dataset for analytics purposes.

#### SQL Query:
```sql

DROP TABLE IF EXISTS Uber_dataset.bigquery_analytics_query;

CREATE TABLE Uber_dataset.bigquery_analytics_query AS
(
  -- SQL Query --
);
```
The SQL query involves joining multiple tables such as `trip_table`, `datetime_dim`, `passenger_count_dim`, `trip_distance_dim`, `ratecode_dim`, `pickup_location_dim`, `dropoff_location_dim`, and `payment_type_dim` to create a denormalized view suitable for analytics.

## Dataset Used

The dataset used in this project comprises trip records from both yellow and green taxi services. It includes essential information such as pick-up and drop-off dates/times, locations, trip distances, fare details, rate types, payment types, and driver-reported passenger counts.

The dataset is sourced from the `TLC Trip Record Data`, which provides comprehensive information about taxi trips in New York City.

The dataset used in the video can be found here - https://github.com/anmol1512/UberData_ETL_Data-Engineering_Project/blob/main/data/uber_data.csv

- Additional Information
For a detailed understanding of the dataset and its attributes, refer to the Data Dictionary - https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

## Config Files
The configuration files in this project play a crucial role in setting up and managing various parameters for the project. Below is the description of the configuration file included:

-`type`: Specifies the type of the service account.
- `project_id`: Your Google Cloud Project ID.
- `private_key_id`: Your private key ID.
- `private_key`: Your private key.
- `client_email`: Your client email.
- `auth_uri`: Your authentication URI.
- `token_uri`: Your token URI.
- `auth_provider_x509_cert_url`: Your authentication -provider x509 certificate URL.
- `client_x509_cert_url`: Your client x509 -
certificate URL.

### Other Configurations
- `GOOGLE_SERVICE_ACC_KEY_FILEPATH`: Filepath to the Google Service Account Key file.
- `GOOGLE_LOCATION`: (Optional) Specifies the location, e.g., "US."

## Requirements 
The project dependencies are specified in the requirements.txt file. Install the dependencies using:

```bash
pip install -r requirements.txt
```
Feel free to explore, contribute, or use the components provided in this repository for your data engineering projects!
## Tech Stack

**Client:** React, Redux, TailwindCSS

**Server:** Node, Express

