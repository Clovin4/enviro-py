import pandas as pd
import numpy as np

from typing import List, Dict

class Transform:
    def __init__(self) -> None:
        self.data = None

    def transform(self, data: pd.DataFrame or Dict(pd.DataFrame)) -> pd.DataFrame or Dict[pd.DataFrame]:
        if type(data) == dict:
            for key, value in data.items():
                data[key] = self._transform(value)
        else:
            data = self._transform(data)
        return data
    
    def _transform(self, data: pd.DataFrame) -> pd.DataFrame:
        data['startTime'] = pd.to_datetime(data['startTime'])
        data['endTime'] = pd.to_datetime(data['endTime'])
        # convert to UTC
        data['startTime'] = data['startTime'].dt.tz_convert('UTC')
        data['endTime'] = data['endTime'].dt.tz_convert('UTC')
        for col in data.columns:
            if isinstance(data[col][0], dict):
                data = self._flatten(data, col)
        return data
    
    def _flatten(self, data: pd.DataFrame, col: str) -> pd.DataFrame:
        df = pd.DataFrame(data[col].apply(pd.Series))
        unit = df['unitCode'][0].split(':')[-1]
        df.rename(columns={'value': f'{col}_{unit}'}, inplace=True)
        data = pd.concat([data, df], axis=1)
        data.drop(columns=[col], inplace=True)
        # drop unitCode column
        data.drop(columns=['unitCode'], inplace=True)
        return data
    