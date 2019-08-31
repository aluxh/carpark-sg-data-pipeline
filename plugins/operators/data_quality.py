from datetime import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Data Quality Checks Operator

    The function will take in params configured in the dag, then check a set of tables under that params.
    The check is count number of rows in staging tables, compared with number of rows inserted into the dimension or fact tables.
    
    If the count is different or table is not found, it will throw an error

    Args:
        redshift_conn_id: Configuration to connect to Redshift
        execution_time: The execution time. This will be used in the SQL statements to determine rows inserted in that specific time.
        table_to_check: The table type: Is it fact or dimension table. The reason why there is a split because 
                        all the dimension tables are updated only once a month on the 25th of the month.
    """
    template_fields = ("execution_time",) # Template fields to ensure that the data is correctly parsed.
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 execution_time="",
                 table_to_check="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.execution_time = execution_time
        self.table_to_check = table_to_check

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        # Convert the time to the right format for used in SQL statement.
        # 1. extract it from the template and convert to datetime format
        # 2. convert into string format
        e_time = datetime.strptime(self.execution_time, '%Y%m%dT%H%M%S')
        e_time = datetime.strftime(e_time, "%Y-%m-%d %H:%M:%S")
        print(e_time)
        
        if (self.table_to_check == "fact"):
            self.log.info('Starting the data quality checks')
            self.log.info('Comparing rainfall table against staging_rainfall table')
            staging_records = redshift.get_records("SELECT COUNT(date_time) FROM public.staging_rainfall;")
            print(f"Staging records: {staging_records}")
            fact_records = redshift.get_records(f"SELECT COUNT(date_time) FROM rainfall_events WHERE date_time = '{ e_time }';")
            print(f"Fact records: {fact_records}")
            if(staging_records[0][0] != fact_records[0][0]):
                raise ValueError("Data Quality check failed. Number of records between staging and fact rainfall_event table is not the same")
            self.log.info(f"Data quality on table rainfall check passed with {fact_records[0][0]} records")
        
            self.log.info('Comparing temperature table against staging_temperature table')
            staging_records = redshift.get_records("SELECT COUNT(date_time) FROM public.staging_temperature;")
            print(f"Staging records: {staging_records}")
            fact_records = redshift.get_records(f"SELECT COUNT(date_time) FROM temperature_events WHERE date_time = '{ e_time }';")
            print(f"Fact records: {fact_records}")
            if(staging_records[0][0] != fact_records[0][0]):
                raise ValueError("Data Quality check failed. Number of records between staging and fact temperature_event table is not the same")
            self.log.info(f"Data quality on table temperature check passed with {fact_records[0][0]} records")

            self.log.info('Comparing carpark availability table against staging carpark availability table')
            staging_records = redshift.get_records("SELECT COUNT(date_time) FROM public.staging_carpark_availability;")
            print(f"Staging records: {staging_records}")
            fact_records = redshift.get_records(f"SELECT COUNT(date_time) FROM carpark_availability WHERE date_time = '{ e_time }';")
            print(f"Fact records: {fact_records}")
            if(staging_records[0][0] != fact_records[0][0]):
                raise ValueError("Data Quality check failed. Number of records between staging and fact carpark_availability table is not the same")
            self.log.info(f"Data quality on table carpark availability check passed with {fact_records[0][0]} records")
        
        elif (self.table_to_check == "dimension"):
            self.log.info('Comparing carpark table against staging_carpark table')
            staging_records = redshift.get_records("SELECT COUNT(carpark_id) FROM public.staging_carpark_info;")
            dimension_records = redshift.get_records("SELECT COUNT(DISTINCT carpark_id) FROM carpark;")
            if(staging_records[0][0] != dimension_records[0][0]):
                raise ValueError("Data Quality check failed. Number of records between staging_carpark_info and dimension carpark table is not the same")
            self.log.info(f"Data quality on table carpark check passed with {dimension_records[0][0]} records")
            
            self.log.info('Comparing weather stations table against staging weather stations')
            staging_records = redshift.get_records("SELECT COUNT(station_id) FROM public.staging_weather_station_info")
            dimension_records = redshift.get_records("SELECT COUNT(station_id) FROM weather_stations;")
            if(staging_records[0][0] != dimension_records[0][0]):
                raise ValueError("Data Quality check failed. Number of records between staging and weather_stations is not the same")
            self.log.info(f"Data quality on table weather_stations check passed with {dimension_records[0][0]} records")

        else:
            raise ValueError("Unknown Table")