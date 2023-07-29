from noaa_sdk import NOAA
import pandas as pd

from typing import List, Dict

class Extract:
    
    def __init__(self) -> None:
        self.noaa = NOAA()
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