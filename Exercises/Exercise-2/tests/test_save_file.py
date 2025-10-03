from main import save_file

def test_save_file(tmp_path):
    #arrange
    output_file=tmp_path / "data.csv"
    #Example of data
    data= (
        "Name,Last modified,Size,Description\n"
        "01002099999.csv,2024-01-19 15:45,178821,\n"
        "01368099999.csv,2024-01-19 15:45,475362,\n"
        "03761099999.csv,2024-01-19 15:45,11077920,\n"
    ).encode("utf-8")

    result = save_file(output_file,data)

    assert result is True 
    assert output_file.exists()