import os.path
import json
import csv

def write_data_to_csv(writer, fileData):
    with open(fileData, "r", encoding="utf-8") as data_json:
                data = json.load(data_json)
                writer.writerow([
                    data['name'], 
                    data['id'], 
                    data['nametype'], 
                    data['recclass'], 
                    data['mass'], 
                    data['fall'], 
                    data['year'], 
                    data['reclat'], 
                    data['reclong'], 
                    data['geolocation']['type'],
                    data['geolocation']['coordinates']
                    ])

def main():
    base_path = os.getcwd()
    csv_file_path = os.path.join(base_path, "Exercises", "Exercise-4", "data_convert.csv")

    #Create csv
    if not os.path.exists(csv_file_path):
        with open(csv_file_path, "w") as f:
            f.write("")
        print("File has created")
    else :
        print("File has existed")

    #Open csv
    with open(csv_file_path, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['name', 'id', 'nametype', 'recclass', 'mass', 'fall', 'year', 'reclat', 'reclong', 'geolocation_type', 'geolocation_coordinates'])                  
        for dirpath, dirnames, filenames in os.walk(base_path):
            for filename in filenames:
                if filename.endswith(".json"):
                    file_path = os.path.join(dirpath, filename)
                    if file_path.endswith(".json"):
                        write_data_to_csv(writer, file_path)
                                    


if __name__ == "__main__":
    main()
