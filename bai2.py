import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
from datetime import datetime
import gzip
import io

def main():
    # Step 1: Get the content of the website
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    print(f"Fetching content from: {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the webpage: {e}")
        return
    
    # Step 2: Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Step 3: Find the file with the specific Last Modified timestamp (2024-01-19 10:27)
    target_timestamp = "2024-01-19 10:27"
    target_file = None
    
    # The webpage typically lists files in a table
    # Each row contains file info including name and last modified date
    rows = soup.find_all('tr')
    
    print(f"Looking for file with timestamp: {target_timestamp}")
    
    for row in rows:
        # Skip header rows
        if row.find('th'):
            continue
        
        # Extract data from columns
        cols = row.find_all('td')
        if len(cols) >= 3:  # Ensure we have enough columns (filename and date)
            filename = cols[0].text.strip()
            modified_date = cols[2].text.strip()
            
            # Check if this is the file we're looking for
            if target_timestamp in modified_date:
                target_file = filename
                print(f"Found matching file: {target_file}")
                break
    
    if not target_file:
        print("Could not find file with the specified timestamp.")
        return
    
    # Step 4: Download the file
    file_url = f"{url}{target_file}"
    print(f"Downloading file from: {file_url}")
    
    try:
        file_response = requests.get(file_url)
        file_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
        return
    
    # Step 5: Save the file locally
    local_filename = target_file
    with open(local_filename, 'wb') as f:
        f.write(file_response.content)
    
    print(f"File saved locally as: {local_filename}")
    
    # Step 6: Load the file with pandas
    # The file might be a CSV or a compressed CSV (gz)
    try:
        if local_filename.endswith('.gz'):
            # For gzipped files
            with gzip.open(local_filename, 'rt') as f:
                df = pd.read_csv(f)
        else:
            # For regular CSV files
            df = pd.read_csv(local_filename)
        
        print("File loaded into pandas successfully.")
        
        # Step 7: Find records with the highest HourlyDryBulbTemperature
        if 'HourlyDryBulbTemperature' in df.columns:
            # Convert to numeric, errors='coerce' will convert non-convertible values to NaN
            df['HourlyDryBulbTemperature'] = pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce')
            
            # Drop rows with NaN in temperature column
            df = df.dropna(subset=['HourlyDryBulbTemperature'])
            
            # Get the maximum temperature
            max_temp = df['HourlyDryBulbTemperature'].max()
            
            # Find all records with the maximum temperature
            max_temp_records = df[df['HourlyDryBulbTemperature'] == max_temp]
            
            print(f"\nRecords with highest HourlyDryBulbTemperature ({max_temp}):")
            print(max_temp_records)
        else:
            print("Column 'HourlyDryBulbTemperature' not found in the dataset.")
    
    except Exception as e:
        print(f"Error processing the file: {e}")
    
    # Clean up - remove the downloaded file
    os.remove(local_filename)
    print(f"Cleaned up: Removed {local_filename}")

if __name__ == "__main__":
    main()