from main import retrive_highest_value

def test_retrive_values_success(tmp_path,capsys):
    #arrange
    csv_file_list = ["data.csv"]
    folder_file= tmp_path / "data.csv"
    #Example of data
    data= (
        "STATION,DATE,LATITUDE,LONGITUDE,ELEVATION,HourlyDryBulbTemperature\n"
        "01368099999,2024-01-19 15:45,61.015556,9.288056,822.04,21\n"
        "01368099999,2024-01-19 15:45,61.015556,9.288056,822.04,18\n"
        "01368099999,2024-01-19 15:45,61.015556,9.288056,822.04,19\n"
    ).encode("utf-8")

    subdata="Row 0: {'STATION': 1368099999, 'DATE': '2024-01-19 15:45', 'LATITUDE': 61.015556, 'LONGITUDE': 9.288056, 'HourlyDryBulbTemperature': 21.0}\n"

    with open(folder_file, 'wb') as f:
        f.write(data)

    retrive_highest_value(tmp_path,csv_file_list,'HourlyDryBulbTemperature')
    
    captured= capsys.readouterr()
    
    assert captured.out == subdata


def test_retrive_values_mix_data_type_success(tmp_path,capsys):
    #arrange
    csv_file_list = ["data.csv"]
    folder_file= tmp_path / "data.csv"
    #Example of data
    data= (
        "STATION,DATE,LATITUDE,LONGITUDE,ELEVATION,HourlyDryBulbTemperature\n"
        "01368099999,2024-01-19 15:45,61.015556,9.288056,822.04,21\n"
        "01368099999,2024-01-19 15:45,61.015556,9.288056,822.04,18s\n"
        "01368099999,2024-01-19 15:45,61.015556,9.288056,822.04,19\n"
    ).encode("utf-8")

    subdata="Row 0: {'STATION': 1368099999, 'DATE': '2024-01-19 15:45', 'LATITUDE': 61.015556, 'LONGITUDE': 9.288056, 'HourlyDryBulbTemperature': 21.0}\n"

    with open(folder_file, 'wb') as f:
        f.write(data)

    retrive_highest_value(tmp_path,csv_file_list,'HourlyDryBulbTemperature')
    
    captured= capsys.readouterr()
    
    assert captured.out == subdata

def test_retrive_values_filter_nan_success(tmp_path,capsys):
    #arrange
    csv_file_list = ["data.csv"]
    folder_file= tmp_path / "data.csv"
    #Example of data
    data= (
        "STATION,DATE,LATITUDE,LONGITUDE,ELEVATION,HourlyDryBulbTemperature\n"
        "01368099999,2024-01-19 15:45,61.015556,9.288056,822.04,21\n"
        "01368099999,2024-01-19 15:45,61.015556,9.288056,822.04,\n"
        "01368099999,2024-01-19 15:45,61.015556,9.288056,822.04,19\n"
    ).encode("utf-8")

    subdata="Row 0: {'STATION': 1368099999, 'DATE': '2024-01-19 15:45', 'LATITUDE': 61.015556, 'LONGITUDE': 9.288056, 'HourlyDryBulbTemperature': 21.0}\n"

    with open(folder_file, 'wb') as f:
        f.write(data)

    retrive_highest_value(tmp_path,csv_file_list,'HourlyDryBulbTemperature')
    
    captured= capsys.readouterr()
    
    assert captured.out == subdata