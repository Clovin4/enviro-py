from etl.extract import Extract
from etl.transform import Transform
from etl.load import Load

if __name__ == '__main__':
    # extract data
    extract = Extract()
    data = extract.get_weather(['70005', '70119'])
    # transform data
    transform = Transform()
    data = transform.transform(data)
    # load data
    load = Load()
    load.load(data)