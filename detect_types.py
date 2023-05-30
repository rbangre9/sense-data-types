import pandas as pd
import random
import sys
import multiprocessing
from dateutil.parser import parse

''' 
this script takes in the path to the source data, and labels the type of each column in the csv.

it does this by testing a random sample of rows and seeing if the proportion of enteries in these rows
for a column is above a certain threshold. If so, the column is deemed to be of that type. Else, it is
given a type of 'not found'

returns a dictionary with the key-value mapping of column-name to type

Example:
    command: py detect_types.py ./example/news_decline.csv 2.9
    output: {'Show': 'string', ' "2009"': 'float', ' "2010"': 'float', ' "2011"': 'float', ' "creation-date"': 'date', ' "overall score"': 'int'}
'''

DATE = "date"
STRING = "string"
INT = "int"
FLOAT = "float"
BOOL = "bool"
NOT_FOUND = "not found"
THRESHOLD = 0.9
SAMPLE_SIZE = 1000
MULTIPROCESS_SIZE = 20000

def is_bool(string):
    string = string.lower()
    if string == 'yes' or string == 'no' or string == 'true' or string == 'false': return True
    return False

def process_column(column_data):
    column_types = {STRING: 0, INT: 0, FLOAT: 0, DATE: 0, BOOL: 0}
    unique_column_data = set(column_data)
    column_length = len(column_data)
    
    if len(unique_column_data) == 2 and all(is_bool(value) for value in unique_column_data): return BOOL
    if pd.api.types.is_integer_dtype(column_data): return INT
    if pd.api.types.is_float_dtype(column_data): return FLOAT

    sample = range(0, column_length) if column_length <= 1000 else random.sample(range(0, column_length), SAMPLE_SIZE)
    
    for i in sample: 
        value = column_data[i]
        try: 
            parse(value)
            column_types[DATE] += 1
        except ValueError: 
            column_types[STRING] += 1

    true_sample_size = len(sample)
    if float(column_types[DATE] / true_sample_size) >= THRESHOLD: return DATE
    if float(column_types[STRING] / true_sample_size) >= THRESHOLD: return STRING
    return NOT_FOUND

def get_types(data):
    return {column: process_column(data[column]) for column in data.columns}

def get_types_mp(data):
    pool = multiprocessing.Pool()
    results = pool.map(process_column, [data[column] for column in data.columns])
    pool.close()
    pool.join()

    return dict(zip(data.columns, results))

def main(data_path):
    data = pd.read_csv(data_path)
    data.columns = data.columns.str.strip()  # Clean column names
    data_size = len(data)
    
    if data_size > MULTIPROCESS_SIZE: 
        print(get_types_mp(data))
    else: 
        print(get_types(data))

if __name__ == "__main__":
    main(sys.argv[1])