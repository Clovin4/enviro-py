import pandas as pd
import requests
from datetime import datetime
import datetime
import yaml

import noaa_sdk as NOAA

from typing import List, Dict

print("Starting extract")
# load config from parent directory
with open('/Users/christianl/repos/enviro-py/config/config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# assign config variables
zipcodes = config['ZIPCODES']
print(zipcodes)

class Extract:
    
    def __init__(self) -> None:
        # self.noaa = NOAA.NOAA()
        self.data = None
        self.country = "US"

    # take in a single zip code or a list of zip codes and return a dictionary of dataframes or dictionary of a dataframe
    def get_weather(self, zip_codes: List[str] or str) -> Dict[str, pd.DataFrame]:
        if isinstance(zip_codes, list):
            self.data = {}
            for zip_code in zip_codes:
                self.data[zip_code] = self._get_weather(zip_code)
        else:
            self.data = {zip_codes: self._get_weather(zip_codes)}
        return self.data
    
    def _get_weather(self, zip_code: str) -> pd.DataFrame:
        # get the data
        data = self.noaa.get_forecasts(zip_code, self.country)
        # convert to dataframe
        data = pd.DataFrame(data)
        return data
    
class Transform:
    def __init__(self) -> None:
        self.data = None

    def dataQualityCheck(self, data: Dict[str, pd.DataFrame]) -> None:
        for key, value in data.items():
            self._dataQualityCheck(value)

    def transform(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        for key, value in data.items():
            data[key] = self._transform(value)
        return data
    
    def _dataQualityCheck(self, data: pd.DataFrame) -> None:
        # check if there are any null values
        if data.isnull().values.any():
            raise ValueError('Data contains null values')
        # check if there are any duplicate rows
        if data.duplicated().any():
            raise ValueError('Data contains duplicate rows')
        # check if there are any negative values
        if (data < 0).any().any():
            raise ValueError('Data contains negative values')
        # check if there are any zero values
        if (data == 0).any().any():
            raise ValueError('Data contains zero values')
        # check if there are any negative values
        if (data < 0).any().any():
            raise ValueError('Data contains negative values')
        # check if there are any negative values
        if (data < 0).any().any():
            raise ValueError('Data contains negative values')
        # check if there are any negative values
        if (data < 0).any().any():
            raise ValueError('Data contains negative values')
        # check if there are any negative values
        if (data < 0).any().any():
            raise ValueError('Data contains negative values')
        # check if there are any negative values
        if (data < 0).any().any():
            raise ValueError('Data contains negative values')
        # check if there are any negative values
        if (data < 0).any().any():
            raise ValueError('Data contains negative values')
        # check if there are any negative values
        if (data < 0).any().any():
            raise ValueError('Data contains negative values')
    
    def _transform(self, data: pd.DataFrame) -> pd.DataFrame:
        data['startTime'] = pd.to_datetime(data['startTime'])
        data['endTime'] = pd.to_datetime(data['endTime'])
        # convert to UTC
        data['startTime'] = data['startTime'].dt.tz_convert('UTC')
        data['endTime'] = data['endTime'].dt.tz_convert('UTC')
        # flatten data
        for col in data.columns:
            if isinstance(data[col][0], dict):
                data = self._flatten(data, col)
        # drop name and number columns
        data.drop(columns=['name', 'number', 'icon', 'detailedForecast'], inplace=True)
        return data
    
    def _flatten(self, data: pd.DataFrame, col: str) -> pd.DataFrame:
        # create dataframe from column
        df = pd.DataFrame(data[col].apply(pd.Series))
        # get unitCode
        unit = df['unitCode'][0].split(':')[-1]
        # rename value column
        df.rename(columns={'value': f'{col}_{unit}'}, inplace=True)
        # concat dataframes
        data = pd.concat([data, df], axis=1)
        # drop column
        data.drop(columns=[col], inplace=True)
        # drop unitCode column
        data.drop(columns=['unitCode'], inplace=True)
        return data
    
class ETL:
    def __init__(self) -> None:
        self.extract = Extract()
        self.transform = Transform()
        self.data = None

    def run(self, zipcodes: List[str]) -> None:
        # extract data
        data = self.extract.get_weather(zipcodes)
        # transform data
        data = self.transform.transform(data)
        # return data
        self.data = data
        return self.data


etl = ETL()
etl.run(zipcodes)