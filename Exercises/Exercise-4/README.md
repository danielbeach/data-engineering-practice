## Exercise #4 - Convert JSON to CSV + Ragged Directories.

In this fourth exercise you will practice your Python skills again,
we will be searching a ragged directory structure, trying to find `json` files.
When `json` files are found, we will then convert them to `csv` files.

We can work with three nice `Python` packages, `glob`, `json`, and `csv`.


#### Setup
1. Change directories at the command line 
   to be inside the `Exercise-4` folder `cd Exercises/Exercise-4`
   
2. Run `docker build --tag=exercise-4 .` to build the `Docker` image.

3. There is a file called `main.py` in the `Exercise-4` directory, this
is where you `Python` code to complete the exercise should go.
   
4. Once you have finished the project or want to test run your code,
   run the following command `docker-compose up run` from inside the `Exercises/Exercise-4` directory

#### Problems Statement
There is a folder called `data` in this current directory, `Exercises/Exercise-4`. There are also
`json` files located at various spots inside this directory structure.

Your task is two use `Python` to find all the `json` files located in the `data` folder.
Once you find them all, read them with `Python` and convert them to `csv` files, to do this
you will have to flatten out some of the nested `json` data structures.

For example there is a `{"type":"Point","coordinates":[-99.9,16.88333]}` that must flattened.

Generally, your script should do the following ...
1. Crawl the `data` directory with `Python` and identify all the `json` files.
2. Load all the `json` files.
3. Flatten out the `json` data structure.
4. Write the results to a `csv` file, one for one with the json file, including the header names.