import pandas as pd
import sys
from dateutil.parser import parse

''' 
this script takes in two command line arguments, 
    
    1. PATH to file 
    2. threshold 

It then computes if the majority of values in each column are of the same type, and if the
proportion of values in the column is higher than the threshold, it returns that type for the column. 

returns a dict of the type of each column (if determinable). If type is not determinable, then returns 'inconclusive'

Example:
    command: py detect_types.py ./example/news_decline.csv 0.9
    output: {'Show': 'string', ' "2009"': 'float', ' "2010"': 'float', ' "2011"': 'float', ' "creation-date"': 'date', ' "overall score"': 'int'}
'''

def is_bool(string):
    string = string.lower()
    if string == 'yes' or string == 'no' or string == 'true' or string == 'false': return True
    return False

def get_types(data_path, threshold):
    data = pd.read_csv(data_path)
    columns = list(data.columns)
    res = {} 

    for column in columns:
        column_types = {"string": 0, "int": 0, "float": 0, "date": 0, "bool": 0}
        column_data = data[column]
        accuracies = {} 

        for value in column_data:
            if isinstance(value, int):
                column_types["int"] += 1
            elif isinstance(value, float): 
                column_types["float"] += 1
            else: 
                try: 
                    parse(value)
                    column_types["date"] += 1
                except ValueError:
                    if is_bool(value): 
                        column_types["bool"] += 1
                    else:
                        column_types["string"] += 1
        
        accuracies["string"] = float(column_types["string"] / len(column_data))
        accuracies["int"] = float(column_types["int"] / len(column_data))
        accuracies["float"] = float(column_types["float"] / len(column_data))
        accuracies["date"] = float(column_types["date"] / len(column_data))
        accuracies["bool"] = float(column_types["bool"] / len(column_data))

        data_type = max(accuracies, key=accuracies.get) if accuracies[max(accuracies, key=accuracies.get)] >= threshold else "inconclusive"
        res[column] = data_type
    
    return res 

if __name__ == "__main__":
    print(get_types(sys.argv[1], float(sys.argv[2])))

