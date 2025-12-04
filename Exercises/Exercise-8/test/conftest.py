import duckdb
import pytest
import csv

@pytest.fixture
def duckdb_conn(tmp_path):
    conn = duckdb.connect(database=':memory:', read_only=False)
    conn.execute("DROP TABLE IF EXISTS electric_vehicule_population;")
    conn.execute(
        "CREATE TABLE electric_vehicule_population(" \
        "VIN VARCHAR," \
        "County VARCHAR," \
        "City VARCHAR," \
        "State VARCHAR," \
        "Postal_Code VARCHAR," \
        "Model_Year BIGINT," \
        "Make VARCHAR," \
        "Model VARCHAR," \
        "Electric_Vehicle_Type VARCHAR," \
        "CAFV_Eligibility VARCHAR," \
        "Electric_Range BIGINT," \
        "Base_MSRP BIGINT," \
        "Legislative_District BIGINT," \
        "DOL_Vehicle_ID BIGINT," \
        "Vehicle_Location VARCHAR," \
        "Electric_Utility VARCHAR," \
        "Census_2020_Tract VARCHAR" \
        ");"
    )
    #create dummy data
    header = ["VIN", 
        "County",
        "City",
        "State",
        "Postal_Code",
        "Model_Year",
        "Make",
        "Model",
        "Electric_Vehicle_Type",
        "CAFV_Eligibility",
        "Electric_Range",
        "Base_MSRP",
        "Legislative_District",
        "DOL_Vehicle_ID",
        "Vehicle_Location",
        "Electric_Utility",
        "Census_2020_Tract"]
    data = [
        ['5YJ3E1EB4L','Yakima','Yakima','WA','98908',2020,'TESLA','MODEL 3','Battery Electric Vehicle (BEV)','Clean Alternative Fuel Vehicle Eligible',322,0,14,127175366,'POINT (-120.56916 46.58514)','PACIFICORP','53077000904'],
        ['5YJ3E1EA7K','San Diego','San Diego','CA','92101',2019,'TESLA','MODEL 3','Battery Electric Vehicle (BEV)','Clean Alternative Fuel Vehicle Eligible',220,0,'',266614659,'POINT (-117.16171 32.71568)','','06073005102'],
        ['7JRBR0FL9M','Lane','Eugene','OR','97404',2021,'VOLVO','S60','Plug-in Hybrid Electric Vehicle (PHEV)','Not eligible due to low battery range',22,0,'',144502018,'POINT (-123.12802 44.09573)','','41039002401'],
        ['5YJXCBE21K','Yakima','Yakima','WA','98908',2019,'TESLA','MODEL X','Battery Electric Vehicle (BEV)','Clean Alternative Fuel Vehicle Eligible',289,0,14,477039944,'POINT (-120.56916 46.58514)','PACIFICORP','53077000401'],
        ['5UXKT0C5XH','Snohomish','Bothell','WA','98908',2017,'BMW','X5','Plug-in Hybrid Electric Vehicle (PHEV)','Not eligible due to low battery range',14,0,1,106314946,'POINT (-122.18384 47.8031)','PUGET SOUND ENERGY INC','53061051918']
        ]
    filename=tmp_path/'dummy_data.csv'
    with open(filename,'w',newline='') as csvfile:
        writer=csv.writer(csvfile)
        writer.writerow(header)
        writer.writerows(data)
    #Insert data into table
    conn.execute(f"COPY electric_vehicule_population FROM '{tmp_path}/dummy_data.csv';")
    yield conn
    conn.close()