from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from helpers.getWeather import get_weather
from helpers.getCarpark import get_carpark
from helpers.getCarparkInfo import get_carparkInfo
from helpers.getWeatherStation import get_weatherStationInfo
from helpers.sql_queries import SqlQueries

from operators.load_to_redshift import LoadS3ToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from operators.facts_calculator import DailyFactsCalculatorOperator
from operators.has_rows import HasRowsOperator

def is_twentyfifth(*args, **kwargs):
        """ Helper function to determine whether it is 25th of each month

        The goal was to identify the time to trigger the downstream process.

        So, the function extracts the execution time from the macros reference 'ts_nodash',
        and convert it into datetime format. Then, compare it against the condition: 
        3 a.m. on the 25th of the month. 

        If it is true, return True, else False.        
        """
        execution_time = datetime.strptime(kwargs['ts_nodash'], '%Y%m%dT%H%M%S')
        if (execution_time.day == 25 and 
                execution_time.hour == 3 and 
                        execution_time.minute == 0):
        # Test whether the time is 3am on 25th of each month.
                return True
        else:
                return False

# Set ui color for Short Circuit Operator
ShortCircuitOperator.ui_color = "#99ccff"

default_args = {
        'owner': 'Alex Ho',
        'start_date': datetime(2019, 8, 24, 23, 0, 0, 0),
        'end_date': datetime(2019, 8, 25, 4, 0, 0, 0),
        'depends_on_past': True,
        'retries': 5,
        'retry_delay': timedelta(minutes=15),
}

dag = DAG(
        dag_id = "carpark_sg_dag",
        schedule_interval = '*/10 * * * *', # 10 mins interval
        max_active_runs=1,
        default_args = default_args
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

load_temperature_to_s3 = PythonOperator(
        task_id='get_temperature_from_api_to_s3',
        python_callable=get_weather,
        provide_context=True,
        params = {
                'table': 'temperature',
                's3_bucket': 'udacity-dend-alex-ho',
                's3_key': 'weather_sg'
        },
        dag=dag
)

load_rainfall_to_s3 = PythonOperator(
        task_id='get_rainfall_from_api_to_s3',
        python_callable=get_weather,
        provide_context=True,
        params = {
                'table': 'rainfall',
                's3_bucket': 'udacity-dend-alex-ho',
                's3_key': 'weather_sg'
        },
        dag=dag
)

load_carpark_availability_to_s3 = PythonOperator(
        task_id='get_carpark_availability_from_api_to_s3',
        python_callable=get_carpark,
        provide_context=True,
        params={
                's3_bucket': 'udacity-dend-alex-ho',
                's3_key': 'carpark_sg'
        },
        dag=dag
)

is_time_of_month = ShortCircuitOperator(
        task_id="is_time_of_month",
        python_callable=is_twentyfifth,
        provide_context=True,
        dag=dag
)

load_carpark_info_to_s3 = PythonOperator(
        task_id='get_carpark_info_from_api_to_s3',
        python_callable=get_carparkInfo,
        provide_context=True,
        params={
                's3_bucket': 'udacity-dend-alex-ho',
                's3_key': 'carpark_sg'
        },
        dag=dag
)

load_weather_stations_info_to_s3 = PythonOperator(
        task_id='get_weather_stations_info_from_api_to_s3',
        python_callable=get_weatherStationInfo,
        provide_context=True,
        params={
                's3_bucket': 'udacity-dend-alex-ho',
                's3_key': 'weather_sg'
        },
        dag=dag
)

stage_temperature_from_s3_to_redshift = LoadS3ToRedshiftOperator(
        task_id='stage_temp_from_s3_to_redshift',
        aws_credentials_id='aws_credentials_id',
        redshift_conn_id='redshift',
        table='staging_temperature',
        s3_bucket='udacity-dend-alex-ho',
        s3_key='weather_sg/temperature_{{ ts_nodash }}.csv',
        dag=dag
)

stage_rainfall_from_s3_to_redshift = LoadS3ToRedshiftOperator(
        task_id='stage_rainfall_from_s3_to_redshift',
        aws_credentials_id='aws_credentials_id',
        redshift_conn_id='redshift',
        table='staging_rainfall',
        s3_bucket='udacity-dend-alex-ho',
        s3_key='weather_sg/rainfall_{{ ts_nodash }}.csv',
        dag=dag
)

stage_carpark_availability_from_s3_to_redshift = LoadS3ToRedshiftOperator(
        task_id='stage_carpark_availability_from_s3_to_redshift',
        aws_credentials_id='aws_credentials_id',
        redshift_conn_id='redshift',
        table='staging_carpark_availability',
        s3_bucket='udacity-dend-alex-ho',
        s3_key='carpark_sg/carpark_{{ ts_nodash }}.csv',
        dag=dag
)

stage_carpark_info_from_s3_to_redshift = LoadS3ToRedshiftOperator(
        task_id='stage_carpark_info_from_s3_to_redshift',
        aws_credentials_id='aws_credentials_id',
        redshift_conn_id='redshift',
        table='staging_carpark_info',
        s3_bucket='udacity-dend-alex-ho',
        s3_key='carpark_sg/carpark_info_{{ ts_nodash }}.csv',
        dag=dag
)

stage_weather_stations_info_from_s3_to_redshift = LoadS3ToRedshiftOperator(
        task_id='stage_weather_stations_info_from_s3_to_redshift',
        aws_credentials_id='aws_credentials_id',
        redshift_conn_id='redshift',
        table='staging_weather_station_info',
        s3_bucket='udacity-dend-alex-ho',
        s3_key='weather_sg/weather_stations_info_{{ ts_nodash }}.csv',
        dag=dag
)

load_temperature_table = LoadFactOperator(
        task_id='load_temperature_events_fact_table',
        redshift_conn_id="redshift",
        table='temperature_events',
        append=True,
        dag=dag
)

load_rainfall_table = LoadFactOperator(
        task_id='load_rainfall_events_fact_table',
        redshift_conn_id="redshift",
        table='rainfall_events',
        append=True,
        dag=dag
)

load_carpark_availability_table = LoadFactOperator(
        task_id='load_carpark_availability_fact_table',
        redshift_conn_id="redshift",
        table='carpark_availability',
        append=True,
        dag=dag
)

load_carpark_table = LoadDimensionOperator(
        task_id='load_carpark_info_dimension_table',
        redshift_conn_id="redshift",
        table='carpark',
        append=False,
        dag=dag        
)

load_weather_station_table = LoadDimensionOperator(
        task_id='load_weather_stations_info_dimension_table',
        redshift_conn_id="redshift",
        table='weather_stations',
        append=False,
        dag=dag        
)

load_time_table = LoadDimensionOperator(
        task_id='load_time_dimension_table',
        redshift_conn_id='redshift',
        table='time',
        append=True,
        dag=dag
)

run_quality_checks_fact = DataQualityOperator(
        task_id='Run_data_quality_checks_fact',
        redshift_conn_id="redshift",
        execution_time='{{ts_nodash}}',
        table_to_check='fact',
        dag=dag
)

run_quality_checks_dimension = DataQualityOperator(
        task_id='Run_data_quality_checks_dimension',
        redshift_conn_id="redshift",
        execution_time='{{ts_nodash}}',
        table_to_check='dimension',
        dag=dag
)

calculate_daily_carpark_stats = DailyFactsCalculatorOperator(
        task_id = "calculate_and_create_daily_carpark_availability_table",
        dag = dag,
        redshift_conn_id="redshift",
        origin_table="carpark_availability",
        destination_table="daily_carpark_stats",
        fact_column="lots_available",
        groupby_column="carpark_id"
)

check_daily_carpark_stats = HasRowsOperator(
        task_id='check_daily_carpark_stats_data',
        dag=dag,
        provide_context=True,
        redshift_conn_id='redshift',
        table='daily_carpark_stats',
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Temperature Workflow
start_operator >> load_temperature_to_s3
load_temperature_to_s3 >> stage_temperature_from_s3_to_redshift
stage_temperature_from_s3_to_redshift >> load_temperature_table
load_temperature_table >> run_quality_checks_fact
run_quality_checks_fact >> end_operator

# Rainfall Workflow
start_operator >> load_rainfall_to_s3 
load_rainfall_to_s3 >> stage_rainfall_from_s3_to_redshift
stage_rainfall_from_s3_to_redshift >> load_rainfall_table 
load_rainfall_table >> run_quality_checks_fact
run_quality_checks_fact >> end_operator

# Carpark Availability Workflow
start_operator >> load_carpark_availability_to_s3 
load_carpark_availability_to_s3 >> stage_carpark_availability_from_s3_to_redshift 
stage_carpark_availability_from_s3_to_redshift >> load_carpark_table
stage_carpark_availability_from_s3_to_redshift >> load_carpark_availability_table 
stage_carpark_availability_from_s3_to_redshift >> load_time_table
load_carpark_availability_table >> run_quality_checks_fact
run_quality_checks_fact >> calculate_daily_carpark_stats
calculate_daily_carpark_stats >> check_daily_carpark_stats
check_daily_carpark_stats >> end_operator
load_time_table >> end_operator

# Carpark Information Workflow (Once a month)
start_operator >> is_time_of_month 
is_time_of_month >> load_carpark_info_to_s3
load_carpark_info_to_s3 >> stage_carpark_info_from_s3_to_redshift
stage_carpark_info_from_s3_to_redshift >> load_carpark_table 
load_carpark_table >> run_quality_checks_dimension 
run_quality_checks_dimension >> end_operator

# Weather Station Information Workflow (Once a month)
is_time_of_month >> load_weather_stations_info_to_s3
load_weather_stations_info_to_s3 >> stage_weather_stations_info_from_s3_to_redshift
stage_weather_stations_info_from_s3_to_redshift >> load_weather_station_table 
load_weather_station_table >> run_quality_checks_dimension
run_quality_checks_dimension >> end_operator