from main import top_3_most_popular_vehicles
import pytest 





def test_top_3_most_popular_vehicles(duckdb_conn):
    

    #Act
    result = top_3_most_popular_vehicles(duckdb_conn).fetchall()

    city_counts = {row[0]: row[1] for row in result}

    expected={
        "TESLA": 3,
        "VOLVO": 1,
        "BMW": 1,
    }
    #Assert
    assert len(city_counts)==3
    assert city_counts == expected