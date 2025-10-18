import csv
import glob 
import json

def flatten_json(y):
    out = {}

    def flatten(x, name=''):

        # If the Nested key-value
        # pair is of dict type
        if type(x) is dict:

            for a in x:
                flatten(x[a], name + a + '_')

        # If the Nested key-value
        # pair is of list type
        elif type(x) is list:

            i = 0

            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    print('JSON file successfully flattened!')
    return out

def json_to_csv_tranformer(root_folder:str):
    """
    Receive the directory or folder where the jsnon files are located.
    Iterate recursively inside the folder searching for json files.
    Compile the json files find into a csv file
    """
    #list to load the json files
    jsonlist=[]
    
    #itereate recursively inside the folder to search for json files
    for fname in glob.glob(f'{root_folder}/**/*.json',recursive=True):
        with open(fname,'r') as f:
            jsonlist.append(flatten_json(json.load(f)))
    
    print('JSON files successfully loaded!')
    
    #compile json files into csv file
    with open(f'{root_folder}/jsonfiles_compiled.csv','w',newline='') as fcsv:
        cw = csv.writer(fcsv)
        c = 0
        for jd in jsonlist:
            if c==0:
                header = jd.keys()
                cw.writerow(header)
                c +=1
            cw.writerow(jd.values())
    
    print(f'CSV file successfully created in {root_folder}')

def main():
    
    json_to_csv_tranformer(root_folder='./data')




if __name__ == "__main__":
    main()
