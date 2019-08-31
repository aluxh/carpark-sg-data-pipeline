import requests
import json
import os
import pandas as pd
import logging

from datetime import datetime
from pandas.io.json import json_normalize
from airflow.contrib.hooks.aws_hook import AwsHook

def get_carpark(*args, **kwargs):
        """
        The function calls the API from data.gov.sg to retrieve Carpark Availability dataset.
        Then, transform the data into rectangular format before saving it as CSV format.
        Lastly, the CSV file will be saved in the S3 bucket.
        
        """
        # Setting up credentials for AWS services
        aws_hook = AwsHook('aws_credentials_id')
        credentials = aws_hook.get_credentials()
        os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key

        # Retrieve S3 bucket and S3 key details from the dag config.
        s3_bucket = kwargs['params']['s3_bucket']
        s3_key = kwargs['params']['s3_key']

        logging.info("Connecting to API to query data...")

        # Extract the execution time, and convert it into the right format, and save it as string format.
        execution_time = datetime.strftime(datetime.strptime(kwargs['ts_nodash'], '%Y%m%dT%H%M%S'), '%Y-%m-%dT%H:%M:%S')

        # Create the parameters to be used to draw data from the API for specific date and time.
        parameters = { 'date_time' : execution_time }

        carpark = requests.get("https://api.data.gov.sg/v1/transport/carpark-availability?", parameters)
        logging.info(f"Data for { execution_time }")

        if carpark.status_code == 200:
                logging.info("Extracting data...")
                carpark_data = carpark.json()

                logging.info("Transforming data...")
                # Take data from the items key
                carpark_data = pd.DataFrame(carpark_data['items'])
                if (carpark_data.empty != True):
                        # Expand nested column - carpark_data into multiple rows and rename the columns.
                        carpark_data = pd.DataFrame([(tup.timestamp, d) for tup in carpark_data.itertuples() for d in tup.carpark_data])
                        carpark_data.rename(columns={0:'timestamp', 1:'readings'}, inplace=True)
                        # Expand nested column - readings into multiple columns
                        carpark_data = pd.concat([carpark_data['timestamp'], json_normalize(carpark_data['readings'])],axis=1)
                        # Expand nested column - carpark_info into multiple rows and rename the columns
                        carpark_data = pd.DataFrame([(execution_time, tup.carpark_number, d) for tup in carpark_data.itertuples() for d in tup.carpark_info])
                        carpark_data.rename(columns={0:'timestamp', 1:'carpark_number', 2:'carpark_info'}, inplace=True)
                        # Expand nested column - carpark_info into multiple columns
                        carpark_data = pd.concat([pd.to_datetime(carpark_data['timestamp']), carpark_data['carpark_number'], json_normalize(carpark_data['carpark_info'])], axis=1)
                        # Format the timestamp column
                        carpark_data['timestamp'] = [x.strftime("%Y-%m-%d %H:%M:%S") for x in carpark_data['timestamp']]

                logging.info("Prepare to save data...")
                # Set the filename based on execution date
                file_name = 'carpark_' + kwargs['ts_nodash'] + '.csv'
                # full_path = os.path.join(os.path.dirname(__file__), 'data', file_name)
                logging.info(f"Retrieve s3 info: {s3_bucket}/{s3_key}")
                s3_path = "s3a://{}/{}/{}".format(s3_bucket, s3_key, file_name)
                
                logging.info(f"Saving { parameters['date_time'] } carpark availability data to { s3_path }")

                # Saving the data as CSV file directly to S3
                carpark_data.to_csv(s3_path, index = False)
                logging.info("Data saved")
        else:
                raise ValueError("Error in the API call")

