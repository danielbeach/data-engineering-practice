import duckdb

REPORT_FOLDER='./reports'

def qty_electric_cars_city(con):
    """
    Count the number of electric cars per city
    """
    query="SELECT City, " \
            "count(1) as Qty_Electric_Cars " \
            "FROM electric_vehicule_population " \
            "GROUP BY City;"
    return con.sql(query)

def top_3_most_popular_vehicles(con):
    """
    Return the 3 most popular electric vehicles
    """
    query="""
        SELECT 
            Make, 
            COUNT(1) as Qty_Vehicles 
        FROM 
             electric_vehicule_population 
        GROUP BY Make 
        ORDER BY Qty_Vehicles desc
        LIMIT 3;
    """
    return con.sql(query)

def most_popular_vehicles_postalcode(con):
    """
    Return the 3 most popular electric vehicles
    """
    query="""
        WITH qty_vehicles as(
            SELECT  
                Postal_Code,
                Make,
                COUNT(1) as Qty_Vehicles 
            FROM electric_vehicule_population 
            GROUP BY Postal_Code, Make
        ),
        vehicles_ranking as(
            SELECT 
                Postal_Code,
                Make,
                Qty_Vehicles,
                RANK() OVER (PARTITION BY Postal_Code ORDER BY Qty_Vehicles DESC) as rank
            FROM qty_vehicles
        )
        SELECT 
            Postal_Code,
            Make,
            Qty_Vehicles
        FROM vehicles_ranking
        WHERE rank=1;

    """
    return con.sql(query)

def qty_electric_cars_model_year(folder,con):
    """
    number of electric cars by model year. 
    Write out the answer as parquet files partitioned by year.
    """
    query = """
        SELECT 
            Model_Year,
            COUNT(1) AS Qty_Electric_Cars
        FROM electric_vehicule_population
        WHERE CAFV_Eligibility = 'Clean Alternative Fuel Vehicle Eligible'
        GROUP BY Model_Year
        
    """

    con.execute(f"COPY ({query}) TO '{folder}/Report_Qty_Electric_Cars_Per_Year' (FORMAT parquet, PARTITION_BY Model_Year, OVERWRITE TRUE)")
    return con.sql(query)

def main():
    # to start an in-memory database
    con = duckdb.connect(database = ":memory:")
    
    #Drop and create the table
    con.execute("DROP TABLE IF EXISTS electric_vehicule_population;")
    con.execute(
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
    #Insert data into table
    con.execute("COPY electric_vehicule_population FROM './data/Electric_Vehicle_Population_Data.csv';")
    
    #queries
    print(qty_electric_cars_city(con))
    print(top_3_most_popular_vehicles(con))
    print(most_popular_vehicles_postalcode(con))
    print(qty_electric_cars_model_year(REPORT_FOLDER,con))




if __name__ == "__main__":
    main()
