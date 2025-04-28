import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Exercise_2.main import scape_local_climatological_data
from datetime import datetime
base_url="https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/",
target_date="2024-01-19 10:27"

a = scape_local_climatological_data(base_url, target_date)
a.process().run()