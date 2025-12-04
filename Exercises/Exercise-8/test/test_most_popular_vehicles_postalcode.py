from main import most_popular_vehicles_postalcode
import pytest 


def test_most_popular_vehicles_postalcode(duckdb_conn):
    

    #Act
    result = most_popular_vehicles_postalcode(duckdb_conn).fetchall()

    postalcode_counts = {row[0]: [row[1],row[2]] for row in result}
    expected= {'92101': ['TESLA', 1], '98908': ['TESLA', 2], '97404': ['VOLVO', 1]}
    #Assert
    assert len(postalcode_counts)==3
    assert postalcode_counts == expected