# import boto3
import glob
import json
from flatten_json import flatten
import pandas as pd
import os


# flatten dictionary inn each json file and return a dictionary
def flatten_file(file):
    with open(file, "rb") as f:
        data = flatten(json.loads(f.read()))

    return data


# transform and save the transformed json file to csv
def transform_toCsv(file, filename):
    data = [file]
    transformed_json = pd.DataFrame.from_dict(data)

    if os.path.isdir("csv"):
        pass

    else:
        os.mkdir("csv")

    transformed_json.to_csv(f"csv/{filename}.csv", index=False)


# find all json files in the data directory and subdirectory
def get_files():
    for json_file in glob.glob("data/**/*.json", recursive=True):
        name = json_file.split("/")[-1].split(".")[0]

        file = flatten_file(json_file)
        transform_toCsv(file, name)

        # return file


def main():
    get_files()


if __name__ == '__main__':
    main()
