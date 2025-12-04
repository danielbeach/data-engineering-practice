from main import qty_electric_cars_city
import pytest 





def test_qty_electric_cars_city(duckdb_conn):
    

    #Act
    result = qty_electric_cars_city(duckdb_conn).fetchall()

    city_counts = {row[0]: row[1] for row in result}

    expected={
        "Yakima": 2,
        "San Diego": 1,
        "Eugene": 1,
        "Bothell": 1,
    }
    #Assert
    assert len(city_counts)==4
    assert city_counts == expected
