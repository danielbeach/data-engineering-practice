## Exercise #1 - Downloading Files with Python.

In this first exercise you will practice your Python skills,
as well as learn about a very common task ... downloading data files
from a `HTTP` source. 
You will have to unzip the files with Python as well.


#### Setup
1. Change directories at the command line 
   to be inside the `Exercise-1` folder `cd Exercises/Exercise-1`
   
2. Run `docker build --tag=exercise-1 .` to build the `Docker` image.

3. There is a file called `main.py` in the `Exercise-1` directory, this
is where you `Python` code to complete the exercise should go.
   
4. Once you have finished the project or want to test run your code,
   run the following command `docker-compose up run` from inside the `Exercises/Exercise-1` directory

#### Problems Statement
You need to download 10 files that are sitting at the following specified
`HTTP` urls. You will use the `Python` package `requests` to do this
work.

You will need to pull the filename from the download uri.

The files are `zip` files that will also need to be unzipped into 
their `csv` format.

They should be downloaded into a folder called `downloads` which
does not exist currently inside the `Exercise-1` folder. You should
use `Python` to create the directory, do not do it manually.

Generally, your script should do the following ...
1. create the directory `downloads` if it doesn't exist
2. download the files one by one.
3. split out the filename from the uri, so the file keeps its 
   original filename.
   
4. Each file is a `zip`, extract the `csv` from the `zip` and delete
the `zip` file.
5. For extra credit, download the files in an `async` manner using the 
   `Python` package `aiohttp`. Also try using `ThreadPoolExecutor` in 
   `Python` to download the files. Also write unit tests to improve your skills.

#### Download URIs are listed in the `main.py` file.

### Hints
1. Don't assume all the uri's are valid.
2. One approach would be the `Python` method `split()` to retrieve filename for uri,
or maybe find the last occurrence of `/` and take the rest of the string.