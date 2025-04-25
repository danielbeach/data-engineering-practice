import os
import glob
import json
import csv

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], f'{name}{a}_')
        elif isinstance(x, list):
            for i, a in enumerate(x):
                flatten(a, f'{name}{i}_')
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def convert_json_to_csv(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except Exception as e:
            print(f"❌ Error loading {json_path}: {e}")
            return

    if isinstance(data, list):
        flat_data = [flatten_json(item) for item in data]
    else:
        flat_data = [flatten_json(data)]

    csv_path = os.path.splitext(json_path)[0] + '.csv'
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=flat_data[0].keys())
        writer.writeheader()
        writer.writerows(flat_data)

    print(f"✅ Converted: {json_path} → {csv_path}")

def main():
    # Tìm tất cả các file .json trong thư mục data/ (kể cả subdirectories)
    json_files = glob.glob('data/**/*.json', recursive=True)

    print(f"📁 Found {len(json_files)} JSON file(s).")

    for json_file in json_files:
        convert_json_to_csv(json_file)

if __name__ == "__main__":
    main()
