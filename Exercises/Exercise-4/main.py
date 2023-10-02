import csv
import glob
import json
import os


def main():
    data_directory = "./data"
    output_csv_file = "output.csv"

    flattened_json_data = load_and_flatten_json_files(data_directory)

    write_flattened_data_to_csv(flattened_json_data, output_csv_file)


def flatten_json(json_obj, parent_key='', separator='_'):
    flattened = {}
    for key, value in json_obj.items():
        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, dict):
            flattened.update(flatten_json(value, new_key, separator=separator))
        else:
            flattened[new_key] = value
    return flattened


def load_and_flatten_json_files(directory):
    flattened_json_data = []

    try:
        if not os.path.exists(directory):
            raise FileNotFoundError("Directory not found")
        json_files = glob.glob(os.path.join(directory, '**', '*.json'), recursive=True)

    except Exception as e:
        raise e

    for json_file in json_files:
        try:
            with open(json_file, 'r') as file:
                json_data = json.load(file)
                flattened_data = flatten_json(json_data)
                flattened_json_data.append(flattened_data)
        except json.JSONDecodeError as e:
            raise e

    return flattened_json_data


def write_flattened_data_to_csv(data, output_csv_file):
    with open(output_csv_file, 'w', newline='') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=data[0].keys())
        csv_writer.writeheader()
        for row in data:
            csv_writer.writerow(row)


if __name__ == "__main__":
    main()
