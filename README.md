# Data model

## Motivation
The data modelling technique adopted for this exercise was something closer to a traditional star schema,
with some small simplifications to make the implementation easier and the model simpler (with less tables).

The reason I've choosen it was to avoid excessive normalization of data, have some of the answers to main questions 
pre-processed and allow the model to be easily used by other layers downstream, such as DWH or BI tools.

## Dimensions and Fact tables

![ERD](https://github.com/diegotmd/bitso-homework/blob/main/Bitso-ERD.png)

- dim_user - Granularity of this dimension is one row for each user, with user_id as primary key and last_login as
           a SCD type 1 column. In a real-world scenario we could enrich this dimension with additional user data,
           so fields could be used for filtering, aggregation and slicing.

- dim_user_jurisdiction - Contains one row for each user and jurisdiction, bringing the current_level as a SCD type 1 column.
                        In the ERD, it's a dimension that references another dimension (dim_user). It's here to make some of
                        the queries easier and also for performance reasons (in case volume of data becomes huge).

- deposit (fact) - It's basically a transaction fact, almost a copy from deposit data in the source, but in a curated layer.
               The reason it's here is to answer adhoc questions that needs data in the most granular level.

- withdrawal (fact) - Same as previous, but with withdrawal data

- user_level (fact) - Similar as the previous fact tables, but in this case we apply some transformations to only keep one register
                    for each user_id and jurisdiction in a specific date. We assumed that the most recent one should be kept if there 
                    are two or more records for the same user_id and jurisdiction in the same date.

- user_daily_snapshot (fact) - A snapshot table with aggregated data to facilitate answering some of the questions in the problem statement,
                             also for performance reasons, in case the volume of data grows a lot. It's a snapshot type fact table and its
                             granularity is user and date with a few metrics aggregated at this level.

- daily_stats (fact) - A snapshot table with aggregated data to facilitate answering some of the questions in the problem statement,
                     also for performance reasons, in case the volume of data grows a lot. It's a snapshot type fact table and its
                     granularity is date, currency, level and jurisdiction with a few metrics aggregated at this level.


## Other dimensions that could be implemented to enrich the data model:
- Jurisdiction
- Currency
- Date

## Queries
You can find queries for some of the questions listed in the problem description [here](https://github.com/diegotmd/bitso-homework/blob/main/queries/queries.sql)

## Benefits of this modelling approach
- Make some of the queries simpler
- Improve performance, since some fact tables have pre-aggregated and pre-transformed data
- Can be easily used as data source inside BI / self-service tools
- Can be used as source for other database layer, using federated tables / materialized view in DW tools such as BigQuery or AWS Redshift
- Keeps granular data in case ad-hoc questions need to be answered from it

## Downsides of this modelling approach
- The transformations and loads for dimension and fact tables can become complex and hard to maintain
- If near real-time data is needed, current processing performance may not be good enough
- Even if it's not very normalized, there is still room for it to  be flattened and have
  higher simplicity and redudancy, making it even easier to users to write their queries.

## Output files (DWH)
All the output files are in the curated data lake layer, you can download the zip file [here](https://github.com/diegotmd/bitso-homework/blob/main/data-lake/curated/curated.zip)

# Explaining Data Lake and Pipelines
The implementation of the pipeline simulates a daily batch processing approach. To make this possible, we had to do some simulation of an environment with daily increments.

## Data Lake Layers

- Landing - It contains the CSVs files provided in the exercise. Imagine that this is an instance of the Postgres database that stores OLTP data. In a real world scenario with daily increments or CDC, this layer would probably not exist.

- Raw - This is the layer that contains raw data for each daily increment. It is here to simulate the daily batches extraction. To fill this layer with data, the first step of the pipeline will get data from landing layer and create the daily increments in corresponding folders. In an environment closer to real world, we would already start with the data from daily increments/batches or something like a CDC.

- Trusted - As the name suggests, this layer contains trusted data after cleaning, deduplication and standardization (important: no business transformations are applied in this layer). For each table, there is a step in the pipeline that reads data from raw layer, apply the mentioned transformations and save data in trusted layer.

- Curated - This layer contains data in the final model (closer to a traditional star schema dimensional model). Even though we are using CSV files in this case, we could easily do this on a different way, either using better file formats for this purpose (such as parquet) or load a traditional Datawarehouse like BigQuery or RedShift. The final step of the pipeline loads this layer with all dimensions and fact tables. 

# List of future improvements
- Increase test coverage, since the solution currently only has tests for the merging between fresh data and DWH (curated) facts and dimensions.
- Replace pandas with a more performant framework, such as Spark or Datawarehouse solutions SQL (BigQuery / RedShift).
- Running as daily batches is not performant for the initial load, so we could fork the code and develop a bulk load for the first run, and only after that the daily pipelines would take place.
- Use an orchestration tool as Airflow to increase control of steps, monitoring, reprocessing, logging, backfilling and others.

# Running tests

## Pre requisites 
You may need to install unittest if not installed yet.

`pip install unittest`

## Running
Run src/tests.py with python3:

`python3 src/tests.py`

# Running the pipelines

## Pre requisites 
All you need to have installed is Python 3 and pandas. If you have Python 3 installed, it usually comes with pip, so to install pandas just run this in a shell:

`pip install pandas`

## Running
The implementation of the pipeline simulates a daily batch processing approach. To make this possible, we had to do some simulation of an environment with daily increments.

We are simulating daily batches, and for this exercise we fixed the starting date as 2020-01-01 and end date as 2023-08-23. You can change this inside `process_etl()` function in `etl.py` file.

The main function that coordinates pipeline execution is in `etl.py`, so to run the code:
- Extract  the content of data-lake/landing.zip inside data-lake/ folder. It will create the required structure in landind layer with source files.
- Run this from a terminal inside root directory: `python3 src/etl.py`

