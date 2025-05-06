import glob
import json
import csv
import os

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], f'{name}{a}_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, f'{name}{i}_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def json_to_csv(json_file_path):
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if isinstance(data, dict):
        data = [data]  # Convert single dict to list of dicts

    flattened_data = [flatten_json(record) for record in data]

    if flattened_data:
        csv_file_path = json_file_path.replace('.json', '.csv')
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=flattened_data[0].keys())
            writer.writeheader()
            writer.writerows(flattened_data)

def main():
    json_files = glob.glob('data/**/*.json', recursive=True)
    for json_file in json_files:
        print(f'Processing {json_file}...')
        json_to_csv(json_file)

if __name__ == "__main__":
    main()
