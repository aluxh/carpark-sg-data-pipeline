import requests
import json
import os
import pandas as pd
import logging

from datetime import datetime
from pandas.io.json import json_normalize
from airflow.contrib.hooks.aws_hook import AwsHook

def get_weatherStationInfo(*args, **kwargs):
        """
        The function calls the API from data.gov.sg to retrieve Weather Stations Information dataset.
        Then, transform the data into rectangular format before saving it as CSV format.
        Lastly, the CSV file will be saved in the S3 bucket.
        """
        logging.info("Connecting to API to query data...")
        
        # Setting up credentials for AWS services
        aws_hook = AwsHook('aws_credentials_id')
        credentials = aws_hook.get_credentials() 
        os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key
        
        # Retrieve S3 bucket and S3 key details from the dag config.
        s3_bucket = kwargs['params']['s3_bucket']
        s3_key = kwargs['params']['s3_key']

        # Extract the execution time, and convert it into the right format, and save it as string format.
        execution_time = datetime.strftime(datetime.strptime(kwargs['ts_nodash'], '%Y%m%dT%H%M%S'), '%Y-%m-%dT%H:%M:%S')
        # Create the parameters to be used to draw data from the API for specific date and time.
        parameters = { 'date_time' : execution_time }

        results = requests.get("https://api.data.gov.sg/v1/environment/relative-humidity?", parameters)

        if results.status_code == 200:
                logging.info("Extracting data...")
                json_data = results.json()

                logging.info("Transforming data...")
                # Take data from the metadata + stations key
                weather_stations = pd.DataFrame(json_data['metadata']['stations'])
                # Expand nested column - location, and combine with other columns
                weather_stations_info = pd.concat([weather_stations.id, weather_stations.name, json_normalize(weather_stations.location)], axis=1)
                # Rename the columns with appropriate names
                weather_stations_info.rename(columns={'id':'station_id', 'name':'station_location', 'latitude':'station_latitude', 'longitude':'station_longitude'}, inplace=True)

                logging.info("Prepare to save data...")
                # Set the filename based on execution date
                file_name = 'weather_stations_info_' + kwargs['ts_nodash'] + '.csv'
                logging.info(f"Retrieve s3 info: {s3_bucket}/{s3_key}")
                
                s3_path = "s3a://{}/{}/{}".format(s3_bucket, s3_key, file_name)
                logging.info(f"Saving { parameters['date_time'] } weather stations information data to { s3_path }")

                # Saving the data as CSV file directly to S3
                weather_stations_info.to_csv(s3_path, index = False)
                logging.info("Data saved")
        else:
                raise ValueError("Error in the API call")

