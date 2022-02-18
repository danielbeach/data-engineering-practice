## Exercise #2 - WebScraping and File Downloading with Python.

In this second exercise you will practice your Python skills again,
we will extend upon the idea of downloading files from `HTTP` sources
with Python, but add a twist.

You will have to "web scrap" a `HTML` page looking for a date, and identifying
the correct file to build a URL with which you can download said file.


#### Setup
1. Change directories at the command line 
   to be inside the `Exercise-2` folder `cd Exercises/Exercise-2`
   
2. Run `docker build --tag=exercise-2 .` to build the `Docker` image.

3. There is a file called `main.py` in the `Exercise-2` directory, this
is where you `Python` code to complete the exercise should go.
   
4. Once you have finished the project or want to test run your code,
   run the following command `docker-compose up run` from inside the `Exercises/Exercise-2` directory

#### Problems Statement
You need to download a file of weather data from a government website.
files that are sitting at the following specified location.

https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/

You are looking for the file that was `Last Modified` on `2022-02-07 14:03`, you
can't cheat and lookup the file number yourself. You must use Python to scrape
this webpage, finding the corresponding file-name for this timestamp, `2022-02-07 14:03`

Once you have obtained the correct file, and downloaded it, you must load the file
into `Pandas` and find the record(s) with the highest `HourlyDryBulbTemperature`.
Print these record(s) to the command line.

Generally, your script should do the following ...
1. Attempt to web scrap/pull down the contents of `https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/`
2. Analyze it's structure, determine how to find the corresponding file to `2022-02-07 14:03` using Python.
3. Build the `URL` required to download this file, and write the file locally.
4. Open the file with `Pandas` and find the records with the highest `HourlyDryBulbTemperature`.
5. Print this to stdout/command line/terminal.
