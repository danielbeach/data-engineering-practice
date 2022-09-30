import glob
import pandas as pd
import os

def main():
    # find json files in 'data' directory
    json_files = glob.glob('data/**/*.json', recursive=True)

    # create csv file from json with same name
    for file in json_files:
        file_name = file.split('.')[-2]
        df = pd.read_json(file)
        df.to_csv(file_name + '.csv')



if __name__ == '__main__':
    main()
