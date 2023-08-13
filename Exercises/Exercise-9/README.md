## Exercise 9 - Learn Lazy computation for Polars.

In this exercise we are to learn out work on larger than memory data sets.
We will use Polars to do this.
https://www.pola.rs/

#### Setup
1. Change directories at the command line 
   to be inside the `Exercise-9` folder `cd Exercises/Exercise-9`
   
2. Run `docker build --tag=exercise-9 .` to build the `Docker` image.

3. There is a file called `main.py` in the `Exercise-9` directory, this
is where your `Polars` code to complete the exercise should go.
   
4. Once you have finished the project or want to test run your code,
   run the following command `docker-compose up run` from inside the `Exercises/Exercise-9` directory

#### Problems Statement
There is a folder called `data` in this current directory, `Exercises/Exercise-9`. Inside this
folder there is a `csv` file. The file is called `202306-divvy-tripdate.csv`. This is an open source
data set bike trips.

Generally the files look like this ...
```
"ride_id","rideable_type","started_at","ended_at","start_station_name","start_station_id","end_station_name","end_station_id","start_lat","start_lng","end_lat","end_lng","member_casual"
"6F1682AC40EB6F71","electric_bike","2023-06-05 13:34:12","2023-06-05 14:31:56",,,,,41.91,-87.69,41.91,-87.7,"member"
```

Your job is to use the `Lazy` functionality of `Polars` to work on this data in a efficient manner.

1. Read the provided `CSV` file into a lazy Dataframe.

3. Calculate the following analytics/problems.
 - Convert all data types to the correct ones.
 - Count the number bike rides per day.
 - Calculate the average, max, and minimum number of rides per week of the dataset.
 - For each day, calculate how many rides that day is above or below the same day last week.
