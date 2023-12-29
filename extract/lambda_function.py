import json
import copy
import boto3
import requests
import pandas as pd 
from datetime import datetime 
from io import StringIO

class WeatherData:
    def __init__(self):
        self.base_url = "http://api.weatherapi.com/v1/current.json"
        self.api_key = "XXXXXXXXXXXX"

    def fetch_data(self, location, aqi="yes"):
        """
        weather data for the particular locations are fetched
        with the help of different parameters like:
        1) API_key , 2) q-> location and 
        3) aqi-> which is already declared 'yes' 
        4)response.get of API will be used to connect database in which 
        base_url and above declared paramters will be passed.
        """
        weather_data_list = []
        for loc in location:
            params = {
                "key": self.api_key,
                "q": loc,
                "aqi": aqi
            }
            response = requests.get(self.base_url, params=params)

            if response.status_code == 200:
                weather_data = response.json()
                weather_data_list.append(weather_data)
            else:
                print(f"Failed to fetch data for location {loc}. Status code: {response.status_code}")
        return weather_data_list
        
    def fetch_location_from_file(self, filename="locations.json"):
        """
        locations for weather data is being fetched from 
        an another file in json format, stored in same directory. 
        """
        with open('locations.json') as f:
            location = json.load(f)
            return location

    

class LocationData(WeatherData):
    def __init__(self):
        super().__init__()

        """
        Here data is being passed from the WeatherData class with the help of inheritance 
        and data engineering is being done ie.
        unwanted data is being removed.
        """

    def remove_unwanted_items(self, weather_data):
        location_list=[]
        for w in weather_data:
            location_dict = w.get("location")
            if 'localtime_epoch' in location_dict:
                del location_dict['localtime_epoch']
            if 'localtime' in location_dict:
                del location_dict['localtime']
            location_list.append(location_dict)
        return location_list 
        

    def location_df(self, location_list):
        """
        after processing, location list in being converted into DataFrame.
        """
        location_dataframe=pd.DataFrame(location_list)
        location_dataframe.index.name='city_id'
        return location_dataframe
    
class CurrentData(WeatherData):
        def __init__(self):
            super().__init__()

        """
        Here data is being passed from the WeatherData class with the help of inheritance 
        and data engineering is being done ie.
        unwanted data is being removed.
        """

        def remove_unwanted_items(self, weather_data):
            current_list=[]
            for c in weather_data:
                current_dict=c.get("current")
                if 'last_updated_epoch' in current_dict:
                    del current_dict['last_updated_epoch']
                if 'condition' in current_dict and 'text' in current_dict['condition']:
                    condition_text = current_dict['condition']['text']
                    current_dict['condition'] = condition_text
                if 'air_quality' in current_dict:
                    del current_dict['air_quality']
                current_list.append(current_dict)
            return current_list
        
        def current_df(self, current_list):
            current_dataframe=pd.DataFrame(current_list)
            current_dataframe.index.name='city_id'
            return current_dataframe
        
class AirQualityData(WeatherData):
            def __init__(self):
                super().__init__()
            """
        Here data is being passed from the WeatherData class with the help of inheritance 
        and data engineering is being done ie.
        unwanted data is being removed.
            """
            
            def remove_unwanted_items(self, weather_data):
                air_quality_list=[]
                for a in weather_data:
                    air_quality_dict=a['current']['air_quality']
                    if 'us-epa-index' in air_quality_dict:
                        del air_quality_dict['us-epa-index']
                    if 'gb-defra-index' in air_quality_dict:
                        del air_quality_dict['gb-defra-index']
                        air_quality_list.append(air_quality_dict)
                return air_quality_list
            
            def air_quality_df(self, air_quality_list):
                air_quality_dataframe=pd.DataFrame(air_quality_list)
                air_quality_dataframe.index.name='city_id'
                return air_quality_dataframe
            
class uploadToS3:
    """
        Here credentials are declared for the AWS S3 bucket
        to store the processed CSV files in the above classes.
    """

    def __init__(self):
        self.s3 = boto3.client(service_name='s3',
                               aws_access_key_id = 'XXXXX!XXX',
                               aws_secret_access_key='XXXXXXXXXXXX')
        
    def upload_to_s3(self, location_df, current_df, air_quality_df):
        """
        here file paths ae declared and separate folder will created as same of uploading date.
        files are being stored in the buffer and then pushed to the S3 as 
        they will not be getting stored in the local directories.
        """

        file_paths= ['location.csv', 'current.csv', 'air_quality.csv']
        upload_date = datetime.now().strftime('%Y-%m-%d') 
        for file_name in file_paths:
            s3_path=f'{upload_date}/{file_name}'
            if file_name == 'location.csv':
                csv_buffer = StringIO()
                location_df.to_csv(csv_buffer)
                content = csv_buffer.getvalue()
                self.s3.put_object(Bucket='weather-api-project-extract-bucket',
                                   Body = content,
                                   Key=s3_path)
                print(f"Successfully uploaded file at {s3_path}")
            if file_name == 'current.csv':
                csv_buffer = StringIO()
                current_df.to_csv(csv_buffer)
                content = csv_buffer.getvalue()
                self.s3.put_object(Bucket='weather-api-project-extract-bucket',
                                   Body = content,
                                   Key=s3_path)
                print(f"Successfully uploaded file at {s3_path}")
            if file_name == 'air_quality.csv':
                csv_buffer = StringIO()
                air_quality_df.to_csv(csv_buffer)
                content = csv_buffer.getvalue()
                self.s3.put_object(Bucket='weather-api-project-extract-bucket',
                                   Body = content,
                                   Key=s3_path)
                print(f"Successfully uploaded file at {s3_path}")

def handler(event, context):
    weather = WeatherData()
    locations = weather.fetch_location_from_file()
    weather_data= weather.fetch_data(location=locations)
    weather_data_copy = copy.deepcopy(weather_data)
    location_data = LocationData()
    location_list=location_data.remove_unwanted_items(weather_data=weather_data)
    location_df=location_data.location_df(location_list=location_list)

    current_data=CurrentData()
    current_list=current_data.remove_unwanted_items(weather_data=weather_data)
    current_df=current_data.current_df(current_list=current_list)

    air_quality_data=AirQualityData()
    air_quality_list=air_quality_data.remove_unwanted_items(weather_data=weather_data_copy)
    air_quality_df=air_quality_data.air_quality_df(air_quality_list=air_quality_list)

    upload_s3 = uploadToS3()
    upload_s3.upload_to_s3(location_df=location_df, current_df=current_df, air_quality_df=air_quality_df)





