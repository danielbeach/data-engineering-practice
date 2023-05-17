## Exercise 8 - Understanding and use DuckDB.

In this exercise we are going to have some problems to solve that will require us to 
use various DuckDB functions and functionality. You can read through the documentation
here https://duckdb.org/docs/

#### Setup
1. Change directories at the command line 
   to be inside the `Exercise-8` folder `cd Exercises/Exercise-8`
   
2. Run `docker build --tag=exercise-8 .` to build the `Docker` image.

3. There is a file called `main.py` in the `Exercise-8` directory, this
is where your `DuckDB` code to complete the exercise should go.
   
4. Once you have finished the project or want to test run your code,
   run the following command `docker-compose up run` from inside the `Exercises/Exercise-8` directory

#### Problems Statement
There is a folder called `data` in this current directory, `Exercises/Exercise-8`. Inside this
folder there is a `csv` file. The file is called `electric-cars.csv`. This is an open source
data set about electric vehicles in the state of Washington.

Generally the files look like this ...
```
VIN (1-10),County,City,State,Postal Code,Model Year,Make,Model,Electric Vehicle Type,Clean Alternative Fuel Vehicle (CAFV) Eligibility,Electric Range,Base MSRP,Legislative District,DOL Vehicle ID,Vehicle Location,Electric Utility,2020 Census Tract
5YJ3E1EB4L,Yakima,Yakima,WA,98908,2020,TESLA,MODEL 3,Battery Electric Vehicle (BEV),Clean Alternative Fuel Vehicle Eligible,322,0,14,127175366,POINT (-120.56916 46.58514),PACIFICORP,53077000904
5YJ3E1EA7K,San Diego,San Diego,CA,92101,2019,TESLA,MODEL 3,Battery Electric Vehicle (BEV),Clean Alternative Fuel Vehicle Eligible,220,0,,266614659,POINT (-117.16171 32.71568),,06073005102
7JRBR0FL9M,Lane,Eugene,OR,97404,2021,VOLVO,S60,Plug-in Hybrid Electric Vehicle (PHEV),Not eligible due to low battery range,22,0,,144502018,POINT (-123.12802 44.09573),,41039002401
```

Your job is to complete each one of the tasks listed below, in order, as they depend on each other.

1. create a DuckDB Table including DDL and correct data types that will hold the data in this CSV file.
 - inspect data types and make DDL that makes sense. Don't just `String` everything.

2. Read the provided `CSV` file into the table you created.

3. Calculate the following analytics.
 - Count the number of electric cars per city.
 - Find the top 3 most popular electric vehicles.
 - Find the most popular electric vehicle in each postal code.
 - Count the number of electric cars by model year. Write out the answer as parquet files partitioned by year.


Note: Your `DuckDB` code should be encapsulated inside functions or methods.

Extra Credit: Unit test your `DuckDB` code.
