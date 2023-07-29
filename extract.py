from noaa_sdk import NOAA
import pandas as pd

from typing import List, Dict

class Extract:
    
    def __init__(self) -> None:
        self.noaa = NOAA()
        self.data = None
        self.country = "US"

    # take in a single zip code or a list of zip codes
    def get_weather(self, zip_codes: str or List[str]) -> pd.DataFrame or Dict[pd.DataFrame]:
        if type(zip_codes) == list:
            df_dict = {}
            for zip_code in zip_codes:
                res = self.noaa.get_forecasts(zip_code, self.country)
                df_dict[zip_code] = pd.DataFrame(res)
            self.data = df_dict
        else:
            res = self.noaa.get_forecasts(zip_codes, self.country)
            self.data = pd.DataFrame(res)
        return self.data