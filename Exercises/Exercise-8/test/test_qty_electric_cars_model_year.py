from main import qty_electric_cars_model_year
import pytest 


def test_qty_electric_cars_model_year(tmp_path,duckdb_conn):
    
    #Arrange
    output_file = tmp_path / 'Report_Qty_Electric_Cars_Per_Year'
    #Act
    result = qty_electric_cars_model_year(tmp_path,duckdb_conn).fetchall()

    year_counts = {row[0]: row[1] for row in result}
    print(year_counts)
    expected= {2019: 2, 2020: 1}
    #Assert
    assert output_file.exists()
    assert len(year_counts)==2
    assert year_counts == expected