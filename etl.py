from etl.extract import Extract
from etl.transform import Transform
from etl.load import Load

import time
from datetime import datetime
# from airflow import DAG
# from airflow.models.dag import task
# from airflow.utils.task_group import TaskGroup
# from airflow.operators.python import PythonOperator

import yaml

# load config
with open('config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# assign config variables
zipcodes = config['ZIPCODES']

if __name__ == '__main__':
    # extract data
    extract = Extract()
    data = extract.get_weather(zipcodes)
    # transform data
    transform = Transform()
    data = transform.transform(data)
    # load data
    load = Load()
    load.load(data)