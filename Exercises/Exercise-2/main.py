import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

class Bai2:
    def __init__(self, base_url, target_date, output_folder="Exercises/Exercise-2"):
        self.base_url = base_url
        self.target_date = target_date
        self.target_datetime = datetime.strptime(target_date, "%Y-%m-%d %H:%M")
        self.output_folder = os.path.join(os.getcwd(), output_folder)
        os.makedirs(self.output_folder, exist_ok=True)

    def find_matching_file(self, soup):
        for link in soup.find_all('a'):
            parent = link.find_parent('tr')
            if parent and self.target_date in parent.text:
                filename = link.get('href')
                return self.base_url + filename
        raise Exception("No file found with that timestamp.")

    def download_file(self, file_url):
        print(f"Downloading: {file_url}")
        response = requests.get(file_url)
        filename = file_url.split('/')[-1]
        local_path = os.path.join(self.output_folder, filename)
        with open(local_path, 'wb') as f:
            f.write(response.content)
        return local_path

    def analyze_max_temperature(self, file_path):
        df = pd.read_csv(file_path)
        if 'HourlyDryBulbTemperature' not in df.columns:
            raise Exception("HourlyDryBulbTemperature column not found in the file.")
        df['HourlyDryBulbTemperature'] = pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce')
        max_temp_row = df.loc[df['HourlyDryBulbTemperature'].idxmax()]
        return max_temp_row

    def process(self):
        response = requests.get(self.base_url)
        soup = BeautifulSoup(response.text, "html.parser")
        file_url = self.find_matching_file(soup)
        file_path = self.download_file(file_url)
        max_temp_record = self.analyze_max_temperature(file_path)
        print("Record with highest HourlyDryBulbTemperature:")
        print(max_temp_record)

if __name__ == "__main__":
    fetcher = Bai2(
        base_url="https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/",
        target_date="2024-01-19 10:27"
    )
    fetcher.process()