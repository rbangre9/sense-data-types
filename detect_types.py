import pandas as pd
import sys
import multiprocessing
from dateutil.parser import parse

''' 
this script takes in two command line arguments, 
    
    1. PATH to file 
    2. threshold 

It then computes if the majority of values in each column are of the same type, and if the
proportion of values in the column is higher than the threshold, it returns that type for the column. 

returns a dict of the type of each column (if determinable). If type is not determinable, then returns 'not found'

Example:
    command: py detect_types.py ./example/news_decline.csv 2.9
    output: {'Show': 'string', ' "2009"': 'float', ' "2010"': 'float', ' "2011"': 'float', ' "creation-date"': 'date', ' "overall score"': 'int'}
'''

DATE = "date"
STRING = "string"
INT = "int"
FLOAT = "float"
BOOL = "bool"
THRESHOLD = 0.9
NOT_FOUND = "not found"

def is_bool(string):
    string = string.lower()
    if string == 'yes' or string == 'no' or string == 'true' or string == 'false': return True
    return False

def process_column(column_data):
    column_types = {STRING: 0, INT: 0, FLOAT: 0, DATE: 0, BOOL: 0}
    unique_column_data = set(column_data)

    if len(unique_column_data) == 2 and all(is_bool(value) for value in unique_column_data): return BOOL
    if pd.api.types.is_integer_dtype(column_data): return INT
    if pd.api.types.is_float_dtype(column_data): return FLOAT

    for value in column_data: 
        try: 
            parse(value)
            column_types[DATE] += 1
        except ValueError:
            column_types[STRING] += 1

    column_length = len(column_data)
    if float(column_types[DATE] / column_length) >= THRESHOLD: return DATE
    if float(column_types[STRING] / column_length) >= THRESHOLD: return STRING
    return NOT_FOUND

def get_types(data_path):
    data = pd.read_csv(data_path)
    data.columns = data.columns.str.strip()  # Clean column names

    pool = multiprocessing.Pool()
    results = pool.map(process_column, [data[column] for column in data.columns])
    pool.close()
    pool.join()

    return dict(zip(data.columns, results))

def main(data_path): 
    print(get_types(data_path))

if __name__ == "__main__":
    main(sys.argv[1])
