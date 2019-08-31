import requests
import json
import os
import pandas as pd
import logging

from datetime import datetime
from pandas.io.json import json_normalize
from airflow.contrib.hooks.aws_hook import AwsHook

def get_weather(*args, **kwargs):
        """
        The function calls the API from data.gov.sg to retrieve weather event dataset.
        Then, transform the data into rectangular format before saving it as CSV format.
        Lastly, the CSV file will be saved in the S3 bucket.
        """
        # Parameters to retrieve different types of dataset from the same API
        settings = {
                'temperature':'air-temperature',
                'rainfall':'rainfall',
                'humidity':'relative-humidity',
                'wind_direction':'wind-direction',
                'wind_speed':'wind-speed'
        }

        # Setting up credentials for AWS services
        aws_hook = AwsHook('aws_credentials_id')
        credentials = aws_hook.get_credentials() 
        os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key

        # Retrieve S3 bucket and S3 key details from the dag config.
        s3_bucket = kwargs['params']['s3_bucket']
        s3_key = kwargs['params']['s3_key']

        # Prepare parameters
        source = kwargs["params"]["table"]
        # Extract the execution time, and convert it into the right format, and save it as string format.
        execution_time = datetime.strftime(datetime.strptime(kwargs['ts_nodash'], '%Y%m%dT%H%M%S'), '%Y-%m-%dT%H:%M:%S')
        # Create the parameters to be used to draw data from the API for specific date and time.
        parameters = { 'date_time' : execution_time }

        logging.info("Connecting to API to query data...")
        results = requests.get(f"https://api.data.gov.sg/v1/environment/{settings[source]}?", parameters)
        
        logging.info(f"Data for { execution_time }")

        if results.status_code == 200:
                logging.info("Extracting data...")
                json_data = results.json()

                logging.info("Transforming data...")
                # Take data from the items key
                json_data = pd.DataFrame(json_data['items'])
                # Expand nested columns into multiple rows.
                json_data = pd.DataFrame([(tup.timestamp, d) for tup in json_data.itertuples() for d in tup.readings])
                # Rename the columns
                json_data.rename(columns={0:'timestamp', 1:'readings'}, inplace=True)
                # Expand nested columns into multiple columns.
                json_data = pd.concat([pd.to_datetime(json_data['timestamp']), json_normalize(json_data['readings'])], axis=1)
                # Format the timestamp column
                json_data['timestamp'] = [x.strftime("%Y-%m-%d %H:%M:%S") for x in json_data['timestamp']]
                
                logging.info("Prepare to save data...")
                logging.info(f"The date_time: {json_data['timestamp'][0]}")
                # Set the filename based on execution date
                file_name = source + '_' + kwargs['ts_nodash'] + '.csv'
                # full_path = os.path.join(os.path.dirname(__file__), 'data', file_name)
                logging.info(f"Retrieve s3 info: {s3_bucket}/{s3_key}")
                s3_path = "s3a://{}/{}/{}".format(s3_bucket, s3_key, file_name)
                
                logging.info(f"Saving { parameters['date_time'] } {source} data to { s3_path }")

                # Saving the data as CSV file directly to S3
                json_data.to_csv(s3_path, index = False)
                logging.info("Data saved")
        else:
                raise ValueError("Error in the API call")

