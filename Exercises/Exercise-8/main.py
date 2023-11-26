import duckdb
import pandas as pd
import seaborn
import glob
import time
import matplotlib
import os


# %%
def main():
    db_work('./result')


def db_work(target_folder):

    if not os.path.exists(target_folder):
        os.makedirs(target_folder)


    conn = duckdb.connect()
    df = conn.execute("""
        SELECT *
        FROM read_csv_auto('data/*.csv', header=True)
    """).df()
    conn.register("df_view", df)
    conn.execute("DESCRIBE df_view").df()

    # Count the number of electric cars per city
    conn.execute("""
    CREATE OR REPLACE TABLE car_per_city AS
        SELECT 
            "City"::VARCHAR AS city,
        FROM df_view
    """)
    conn.execute("""
        CREATE OR REPLACE TABLE electric_car_per_city AS
            SELECT 
                City, COUNT(*) as ElectricCarCount
                FROM car_per_city
                GROUP BY City;
    """)
    conn.execute("COPY (FROM electric_car_per_city) TO '{}/electric_car_per_city.parquet' (FORMAT 'parquet')".format(target_folder))

    # Find the top 3 most popular electric vehicles.
    conn.execute("""
    CREATE OR REPLACE TABLE top_tree_car AS
        SELECT 
            "Make"::VARCHAR AS make,
        FROM df_view
    """)
    conn.execute("""
    CREATE OR REPLACE TABLE top_cars AS
        SELECT Make, COUNT(*) as ModelCount
        FROM top_tree_car
            GROUP BY Make
            ORDER BY ModelCount DESC
            LIMIT 3;
    """)
    conn.execute("COPY (FROM top_cars) TO '{}/top_cars.parquet' (FORMAT 'parquet')".format(target_folder))

    # Find the most popular electric vehicle in each postal code.
    conn.execute("""
    CREATE OR REPLACE TABLE top_car_by_postcode AS
        SELECT 
            "Make"::VARCHAR AS make,
            "Postal Code"::INTEGER AS postal_code
        FROM df_view
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE vehicle_by_postcode AS
        WITH RankedMakes AS (
            SELECT
                postal_code,
                make,
                RANK() OVER (PARTITION BY postal_code ORDER BY COUNT(*) DESC) AS rnk
                    FROM
                        top_car_by_postcode
                    GROUP BY
                        postal_code, make)
                SELECT
                    postal_code,
                    make
                FROM RankedMakes
                    WHERE
                    rnk = 1;
    """)
    conn.execute("COPY (FROM vehicle_by_postcode) TO '{}/vehicle_by_postcode.parquet' (FORMAT 'parquet')".format(target_folder))

    # Count the number of electric cars by model year.
    conn.execute("""
    CREATE OR REPLACE TABLE cars_by_year AS
        SELECT 
            "Make"::VARCHAR AS make,
            "Model Year"::INTEGER AS model_year
        FROM df_view
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE car_by_model_year AS
            SELECT
                "model_year", COUNT(DISTINCT "make") AS unique_make_count
                FROM cars_by_year
                GROUP BY "model_year"
    """)
    conn.execute("COPY (FROM car_by_model_year) TO '{}/car_by_model_year.parquet' (FORMAT 'parquet')".format(target_folder))


if __name__ == "__main__":
    main()
