import requests
import json
import os
import pandas as pd
import logging

from airflow.contrib.hooks.aws_hook import AwsHook

def get_carparkInfo(*args, **kwargs):
        """
        The function calls the API from data.gov.sg to retrieve Carpark Information dataset.
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

        logging.info("Connecting to API to query data")
        carpark_info = requests.get('https://data.gov.sg/api/action/datastore_search?resource_id=139a3035-e624-4f56-b63f-89ae28d4ae4c')

        if carpark_info.status_code == 200:
                logging.info("Extracting data...")
                carpark_info_data = carpark_info.json()

                logging.info("Transforming data...")
                # Take data from results + records
                carpark_info_data = pd.DataFrame(carpark_info_data['result']['records'])
                if (carpark_info_data.empty != True):
                        carpark_info_data = carpark_info_data[['car_park_no', 'address', 'y_coord', 'x_coord']]
                        carpark_info_data.rename(columns={'car_park_no':'carpark_id',
                                                        'address':'carpark_location',
                                                        'y_coord':'carpark_latitude',
                                                        'x_coord':'carpark_longitude'}, inplace=True)

                        logging.info("Prepare the save data...")
                        # Set the filename based on execution date
                        file_name = 'carpark_info_' + kwargs['ts_nodash'] + '.csv'
                        s3_path = "s3a://{}/{}/{}".format(s3_bucket, s3_key, file_name)

                        logging.info(f"Saving carpark information data to { s3_path }")
                        # Saving the data as CSV file directly to S3
                        carpark_info_data.to_csv(s3_path, index=False)

                        logging.info("Data saved")
                else:
                        raise ValueError("There are no data returned")
        else:
                raise ValueError("Error in the API call")

                

