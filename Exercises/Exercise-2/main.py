import requests
import pandas as pd
import os
from bs4 import BeautifulSoup


def main():
    # your code here
    URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    FILE_DATE = "2022-02-07 14:03"
    folder = "downloads"
    folder_path = "./" + folder


    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")

    target = soup.find_all(lambda tag:tag.name=='tr' and FILE_DATE in tag.text)

    #if many was found, use the first one
    if(len(target) > 1):
        target = target[0]

    filename = target.find('a')['href']
    file_url = '%s%s' % (URL, filename)

    #download file
    r = requests.get(file_url)
    if not r.status_code == 200:
        print('URL: %s Status %s' % (file_url, r.status_code))
        return

    if not os.path.exists(folder_path):
        print("Folder doesn't exist")
        os.makedirs(folder_path)

    file_path = folder_path + "/" + filename
    f = open(file_path, "wb")
    f.write(r.content)
    f.close()


    #load in pandas and find the highest HourlyDryBulbTemperature
    df = pd.read_csv(file_path)

    target_col = "HourlyDryBulbTemperature"
    df[df[target_col] == df[target_col].max()] \
    .apply(lambda x: print('Max temperature was %s on %s at %s' % (x['HourlyDryBulbTemperature'], x['DATE'], x['NAME'])),1)


if __name__ == '__main__':
    main()
