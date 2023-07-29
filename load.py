from extract import Extract
from transform import Transform
import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

from typing import List, Dict

class Load:
    def __init__(self) -> None:
        self.data = None

    def load(self, data: pd.DataFrame or Dict[pd.DataFrame]) -> None:
        if type(data) == dict:
            for key, value in data.items():
                self._load(key, value)
        else:
            self._load(data)
    
    def _load(self, key: str ,data: pd.DataFrame) -> None:
        # create connection to database
        engine = sqlalchemy.create_engine('sqlite:///data.sqlite')
        # create session
        Session = sessionmaker(bind=engine)
        session = Session()
        # create table with the name of the key
        data.to_sql(key, con=engine, if_exists='replace')
        # commit changes
        session.commit()
        # close session
        session.close()

if __name__ == '__main__':
    # extract data
    extract = Extract()
    data = extract.get_weather(['39440', '70119'])
    # transform data
    transform = Transform()
    data = transform.transform(data)
    # load data
    load = Load()
    load.load(data)