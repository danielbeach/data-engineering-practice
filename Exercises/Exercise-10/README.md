## Exercise 10 - Data Quality with Great Expectations

In this exercise we are to learn about data quality and data quality checks, 
specifcally with a tool called Great Expectations. https://greatexpectations.io/
We have a datasets in Postgres along with an existing data pipeline that is having
data quality issues, you need to implement data quality checks to catch these problems.

#### Setup
1. Change directories at the command line 
   to be inside the `Exercise-10` folder `cd Exercises/Exercise-10`
   
2. Run `docker build --tag=exercise-10 .` to build the `Docker` image.

3. There is a file called `main.py` in the `Exercise-10` directory, this
is where the data pipeline exists and the data quality checks need to go.
   
4. Once you have finished the project or want to test run your code,
   run the following command `docker-compose up run` from inside the `Exercises/Exercise-10` directory
   The code should raise some data quality errors.

#### Problems Statement
There is a folder called `data` in this current directory, `Exercises/Exercise-10`. Inside this
folder there is a `csv` file. The file is called `202306-divvy-tripdate.csv`. This is an open source
data set bike trips.

Generally the files look like this ...
```
"ride_id","rideable_type","started_at","ended_at","start_station_name","start_station_id","end_station_name","end_station_id","start_lat","start_lng","end_lat","end_lng","member_casual"
"6F1682AC40EB6F71","electric_bike","2023-06-05 13:34:12","2023-06-05 14:31:56",,,,,41.91,-87.69,41.91,-87.7,"member"
```

Recently the analytics that have been calculating the max bike trip durations have shown some very strange
and long trip durations. It's expected that most bike trips start and end on the same day. We need to 
implement a Data Quality alert that will let us know when we are getting erroneous durations in ride times.

1. Use Great Expectations to satisfy this requirement.
2. The current dataset includes erroneous trip lenghts, when you run this pipeline (using `docker-compose up run`) 
your data quality checks should pick up on this.


If you don't know where to start check out this blog post https://www.confessionsofadataguy.com/great-expectations-with-apache-spark-a-tale-of-data-quality/


