import boto3
import psycopg2
import pandas as pd
from io import BytesIO
from datetime import datetime


class Connect2Db:
    """
    Database is connected with the following credentials
    """
    def __init__(self):
        self.host = "mydatabase-instance.abc123xyz456.us-east-1.rds.amazonaws.com"
        self.user = "postgres"
        self.password = "Varun#123"
        self.dbname = "weather_api_project"
        self.conn = psycopg2.connect(
            host=self.host, user=self.user, password=self.password, dbname=self.dbname
        )
        self.cursor = self.conn.cursor()

    def insert_data(self, df, table_name):
        """
        fetched data is inserted in the following tables
        """
        columns = ",".join(df.columns)
        values = ",".join(["%s" for _ in df.columns])
        
        df = df.where(pd.notna(df), None)
    

        data_values = [tuple(row) for _, row in df.iterrows()]

        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        self.cursor.executemany(query, data_values)

        self.conn.commit()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

class pulls3:
    """
    connection with the S3 bucket is made with the help of the following credentials
    """

    def __init__(self):
        self.s3 = boto3.client(
            service_name='s3',
                               aws_access_key_id = 'AKIAZKCVMN7GCNMWKQ4I',
                               aws_secret_access_key='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'
        )
        self.db_connector = Connect2Db()  

    def fetch_data_from_s3(self, bucket_name, upload_date, table_names):
        """
        Data is fetched from the S3 with the current date folder name
        """
        s3_prefix = f'{upload_date}/'

        response = self.s3.list_objects(Bucket=bucket_name, Prefix=s3_prefix)
    
        file_keys = [obj['Key'] for obj in response.get('Contents', [])]

        for file_key in file_keys[::-1]:
            response = self.s3.get_object(Bucket=bucket_name, Key=file_key)
            file_content = response['Body'].read()

            df = pd.read_csv(BytesIO(file_content))
            df = df[df.columns[1:]]
            df = self.update_column_name(file_key, df)
           

            # Insert data into tables
            for table_name in table_names:
                if table_name == file_key.split('/')[1].split('.')[0]:
                    self.db_connector.insert_data(df, table_name)
                    print('Data Successfully inserted into: ' ,table_name)

    def update_column_name(self, file_path, df):
        """
        column of the CSV file fetched from S3 is updated to match the column name in the database
        """
        if file_path.split('/')[1].split('.')[0] == 'location':
            location_name_mapping={
                    'name':'city_name',
                    'tz_id':'timezone',
                    'lon':'long'
                }
            df.columns = [location_name_mapping[col] if col in location_name_mapping else col for col in df.columns]
        return df
    
def handler(event, context):
    bucket_name = 'weather-api-project-extract-bucket'
    upload_date = datetime.now().strftime('%Y-%m-%d')
    table_names = ['location', 'air_quality','current']

    data_fetcher = pulls3()
    data_fetcher.fetch_data_from_s3(bucket_name, upload_date, table_names)