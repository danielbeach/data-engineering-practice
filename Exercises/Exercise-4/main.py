import csv
import os
import json
import csv

def main():
    MY_PATH = "./data"
    #walk trhu dictories
    for root, dirs, files in os.walk(MY_PATH):
        for file in files:
            #is a json file
            if(file.endswith('.json')):
                json_to_csv(root, file)

def json_to_csv(folder, file):
    print(folder, file)
    with open(f"{folder}/{file}", 'r') as f_json:
        json_obj = json.load(f_json)
        json_obj = flatten_json(json_obj)

        columns = [x for x in json_obj]
        values = [json_obj[x] for x in json_obj]

        csv_filename = file.replace('.json','.csv')
        print (csv_filename)

        with open(f"{folder}/{csv_filename}", 'w') as f_csv:
            csv_w = csv.writer(f_csv)
            csv_w.writerow(columns)
            csv_w.writerow(values)



def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

if __name__ == '__main__':
    main()
