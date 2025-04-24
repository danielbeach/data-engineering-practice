import os.path
import requests
import os
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

def main():
    # Step 1: Scrape directory listing
    base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Step 2: Parse for file with corresponding date
    target_date = "2024-01-19 10:27"
    target_datetime = datetime.strptime(target_date, "%Y-%m-%d %H:%M")

    # The file list is in <pre> formatted text, usually with anchor tags <a>
    # But here we'll use a trick to find the exact file by matching the datetime string
    matching_url = None

    for link in soup.find_all('a'):
        parent = link.find_parent('tr')
        if parent and target_date in parent.text:
            filename = link.get('href')
            matching_url = base_url + filename
            break

    if not matching_url:
        raise Exception("No file found with that timestamp.")

    print(f"Downloading: {matching_url}")

    # Step 3: Download the file
    file_response = requests.get(matching_url)
    filename = matching_url.split('/')[-1]
    local_filename = os.path.join(os.getcwd(), "Exercises", "Exercise-2", filename)
    with open(local_filename, 'wb') as f:
        f.write(file_response.content)

    # Step 4: Load with pandas and find max HourlyDryBulbTemperature
    df = pd.read_csv(local_filename)
    if 'HourlyDryBulbTemperature' not in df.columns:
        raise Exception("HourlyDryBulbTemperature column not found in the file.")

    # Clean the temperature column if needed (e.g. remove 's' or other annotations)
    df['HourlyDryBulbTemperature'] = pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce')

    max_temp_row = df.loc[df['HourlyDryBulbTemperature'].idxmax()]

    print("Record with highest HourlyDryBulbTemperature:")
    print(max_temp_row)


if __name__ == "__main__":
    main()