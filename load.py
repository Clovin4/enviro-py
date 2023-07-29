# load class to load data into database 

import pandas as pd
import numpy as np

from typing import List, Dict

class Load:
    def __init__(self) -> None:
        self.data = None

    def load(self, data: pd.DataFrame or Dict[pd.DataFrame]) -> None:
        if type(data) == dict:
            for key, value in data.items():
                self._load(value)
        else:
            self._load(data)
    
    def _load(self, data: pd.DataFrame) -> None:
        pass