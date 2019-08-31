# Parking Lots Availability and Forecasting App

The goal of this project is to build data pipeline for gathering real-time carpark lots availability and weather datasets from Data.gov.sg. These data are extracted via API, and stored them in the S3 bucket before ingesting them into the Dare Warehouse. These data will be used to power the mechanics of the Parking Lots Availability and Forecasting App

The objectives are to:

1. Building an ETL pipeline using Apache Airflow to extract data via API and store them in AWS S3.
2. Ingesting data from AWS S3, and staging them in Redshift, and
3. Transforming data into a set of dimensional and fact tables.

## Getting started

The data pipeline is developed using Docker containers, where we could deployed the technology stack locally and in the cloud. The technology stack used for building the pipeline are:

- __Docker Containers:__
  - __Postgres:__ Deployed 2 instances, one for the Airflow metadata db, and the other for initial development of data warehouse.
  - __Pgadmin:__ PostgreSQL Db administrative tool
  - __Jyupter Notebook:__ Development environment for automating the data warehouse deployment, developing ETL codes, and run data exploration.
  - __Airflow:__ Designing and deploying codes as workflows.
- __AWS S3:__ AWS S3 for building the Data Lake
- __AWS Redshift:__ AWS Redshift for Data Warehouse

### Prerequisites

- You need to have docker installed on your local device or AWS EC2 containers before you beging
- You need to create an admin user in your AWS account, then include your key and secret to the `access.cfg`.
- You need to include all the parameters in `dwh.cfg` for connecting to your redshift cluster and database.

__Note:__ In the **DWH** section in the `dwh.cfg`, we have included some configurations for creating the Redshift cluster automatically in `create_redshift.py` script. You can modify the configuration for your needs.

__Note:__ We have included some configuration of the tech stack in `docker-compose.yml` that I'm using for building the pipelines. You can modify the file to customize your pipeline.

## Folders and Files

- __dags:__ Contains all the airflow dags.
  - __carparksg_dag.py:__ This contains the data pipeline for this project.
- __plugins:__
  - __helpers:__ Folders storing helper functions for the data pipeline.
    - __getCarpark.py:__ Helper function to extract carpark availability data via API, transform and store the dataset in S3 buckets in CSV format.
    - __getCarparkInfo.py:__ Helper function to extract the information about each carpark via API, transform and store the dataset in S3 buckets in CSV format.
    - __getWeather.py:__ Helper function to extract temperature and rainfall data via API, transform and store the dataset in S3 buckets in CSV format.
    - __getWeatherStation.py:__ Helper function to extract the information about the weather stations via API, transform and store the dataset in S3 buckets in CSV format.
    - __sql_queries.py:__ Helper function to transform data from staging tables to dimension and fact tables in AWS Redshift.
  - __operators:__ Folders storing Airflow custom operators for the data pipeline.
    - __data_quality.py:__ The data quality operator to run checks on the data stored in the Redshift.
    - __facts_calculator.py:__ The custom operator to run statistic summary on carpark availability with daily partitioning.
    - __has_rows.py:__ The custom operator to check and ensure that the table doesn't contain empty rows.
    - __load_dimension.py:__ The custom operator to load data from staging tables to dimension tables in AWS Redshift
    - __load_fact.py:__ The custom operator to load data from staging and dimension tables to fact tables in AWS Redshift
    - __load_to_redshift.py:__ The custom operator to load data from S3 to staging tables in AWS Redshift
    - __load_to_s3.py:__ The custom operator to load data from API calls, transform and saved them in AWS S3 buckets.
- __logs:__ Folder for storing airflow logs.
- __notebooks:__ Folder for storing the development codes
  - __create_redshift.py:__ Python script for creating a Redshift cluster.
  - __create_tables.py:__ Python script for creating tables in the Redshift data warehouse.
  - __delete_redshift.py:__ Python script for deleting the Redshift cluster
  - __sql_stmt_create_tables.py:__ SQL helper functions for create_tables.py to create tables in Redshift.
  - __access.cfg:__ Configuration file that contains the AWS access and secret keys
  - __dwh.cfg:__ Configuration file that contains the data warehouse configuration.
  - __etl.ipynb:__ ETL development notebook
  - __redshift_connect.ipynb:__ Redshift Configuration/Connection notebook
- __create_tables.sql:__ SQL scripts for creating tables in PostgreSQL or Redshift
- __docker-compose.yml:__ Docker configuration file for deploying containers (technology stacks)
- __requirements.txt:__ Package to be installed in the airflow container

## Setup

Run the docker environment from the `carpark-sg` folder:

```bash
docker-compose up -d

> Creating network "carpark-sg_default" with the default driver
> Creating carpark-sg_postgres_1 ... done
> Creating carpark-sg_pg-data_1  ... done
> Creating carpark-sg_jupyter_1  ... done
> Creating carpark-sg_webserver_1 ... done
> Creating carpark-sg_pgadmin_1   ... done

```

After it stops, you can point your browser to:

- localhost:8080 to access Airflow
- localhost:8888 to access Jyupter Notebook
- localhost:80 to access pgadmin

From Jyupter Notebook, create a new terminal and run:

```
python create_redshift.py

> 1. Fetch params
> 2. Setup Clients and resources
> 3.1 Creating a new IAM Role
> 3.2 Attaching Policy
> 3.3 Get the IAM role ARN
> 4. Creating Redshift Cluster
> Redshift is creating
> ..
> Redshift is available
> ..
>                  Key                                              Value
> 0  ClusterIdentifier                                         dwhcluster
> 1           NodeType                                          dc2.large
> 2      ClusterStatus                                          available
> 3     MasterUsername                                            dwhuser
> 4             DBName                                                dwh
> 5           Endpoint  {'Address': 'dwhcluster.crttik8cimnv.us-west-2...
> 6              VpcId                                       vpc-789b3500
> 7      NumberOfNodes                                                  4
> ..
> 5. Setup incoming TCP port...
> ..
> DWH_ENDPOINT :: dwhcluster.crttik8cimnv.us-west-2.redshift.amazonaws.com
> DWH_ROLE_ARN :: arn:aws:iam::996990424048:role/dwhRole
```

The script will stop after the cluster is created. Then, you can move on to setup the database and tables:

```
%run create_tables

> Creating Tables in Redshift
> Tables are created in Redshift
```

After the tables is setup, you can access airflow via localhost:8080, and begin the data pipeline by switching on the `carpark_sg_dag` on the dashboard.

### Delete the Redshift cluster

If you decide to stop using Redshift, you can delete the cluster by running:

```
%run delete_redshift

> 1. Fetch params
> 2. Setup Clients
> 3. Deleting Redshift Clusters
> Redshift is deleting
> Redshift is deleting
..
> Redshift is deleted
> 4. Clean up Resources
```

The script will stop once Redshift cluster is deleted.

## Schema for staging tables

We gathered data from carpark availability, carpark info, temperature and rainfall dataset, and dump all of them to the staging servers:

#### __staging_temperature__

| NAME | DATA TYPE |
|:-----|:----------|
| date_time | TIMESTAMPTZ NOT NULL |
| station_id | VARCHAR |
| temperature | DOUBLE PRECISION |

#### __staging_rainfall__

| NAME | DATA TYPE |
|:-----|:----------|
| date_time | TIMESTAMPTZ NOT NULL |
| station_id | VARCHAR |
| rainfall | DOUBLE PRECISION |

#### __staging_carpark_availability__

| NAME | DATA TYPE |
|:-----|:----------|
| date_time | TIMESTAMPTZ NOT NULL |
| carpark_id | VARCHAR |
| lot_type | VARCHAR |
| lots_available | INTEGER |
| total_lots | INTEGER |

#### __staging_weather_station_info__

| NAME | DATA TYPE |
|:-----|:----------|
| station_id | VARCHAR |
| station_location | VARCHAR |
| station_latitude | DOUBLE PRECISION |
| station_longitude | DOUBLE PRECISION |

#### __staging_carpark_info__

| NAME | DATA TYPE |
|:-----|:----------|
| carpark_id | VARCHAR |
| carpark_location | VARCHAR |
| carpark_latitude | DOUBLE PRECISION |
| carpark_longitude | DOUBLE PRECISION |

#### __temperature_events__

Temperature events. Setting *station_id* as __FOREIGN KEY__ referencing to *weather_statons*. On top of that, configure the distribution style as __KEY__ and compound sort key using *date_time* and *station_id* to improve _join_ and _group by_ performance.

| NAME | DATA TYPE |
|:-----|:----------|
| date_time | TIMESTAMPTZ NOT NULL |
| station_id | VARCHAR REFERENCES weather_stations (station_id) |
| temperature | DOUBLE PRECISION |

#### __rainfall_events__

Rainfall events. Setting *station_id* as __FOREIGN KEY__ referencing to *weather_statons* table. On top of that, configure the distribution style as __KEY__ and compound sort key using *date_time* and *station_id* to improve _join_ and _group by_ performance.

| NAME | DATA TYPE |
|:-----|:----------|
| date_time | TIMESTAMPTZ NOT NULL |
| station_id | VARCHAR REFERENCES weather_stations (station_id) |
| rainfall | DOUBLE PRECISION |

#### __carpark_availability__

Carpark Availability events. Setting *carpark_id* as __FOREIGN KEY__ referencing to *carpark* table. On top of that, configure the distribution style as __KEY__ and compound sort key using *date_time* and *carpark_id* to improve _join_ and _group by_ performance.

| NAME | DATA TYPE |
|:-----|:----------|
| date_time | TIMESTAMPTZ NOT NULL |
| carpark_id | VARCHAR REFERENCES carpark (carpark_id) |
| lots_available | INTEGER |

#### __weather_station__

Weather stations in weather events database. Setting *station_id* as __PRIMARY KEY__ constraint

| NAME | DATA TYPE |
|:-----|:----------|
| station_id | VARCHAR PRIMARY KEY |
| station_location | VARCHAR |
| station_latitude | DOUBLE PRECISION |
| station_longitude | DOUBLE PRECISION |

#### __carpark__

Carparks in carpark availability database. Setting *carpark_id* as __PRIMARY KEY__ constraint

| NAME | DATA TYPE |
|:-----|:----------|
| carpark_id | VARCHAR PRIMARY KEY |
| carpark_location | VARCHAR |
| carpark_latitude | DOUBLE PRECISION |
| carpark_longitude | DOUBLE PRECISION |
| total_lots | INTEGER |

#### __time__

Timestamps of records in carpark availability broken down into specific units. Setting *start_time* as **PRIMARY KEY**.

| NAME | DATA TYPE |
|:-----|:----------|
| date_time | TIMESTAMPTZ PRIMARY KEY |
| hour | INTEGER |
| day | INTEGER |
| week | INTEGER |
| month | INTEGER |
| weekday | INTEGER |
