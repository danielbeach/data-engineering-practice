## Exercise 7 -Using Various PySpark Functions

In this exercise we are going to have some problems to solve that will require us to 
use various PySpark functions. You should only use functions provided in `spark.sql.functions`.
Do not use UDF's or Python methods to solve the problems! We will be using open source
hard-drive failure data files as our source data.

#### Setup
1. Change directories at the command line 
   to be inside the `Exercise-7` folder `cd Exercises/Exercise-7`
   
2. Run `docker build --tag=exercise-7 .` to build the `Docker` image.

3. There is a file called `main.py` in the `Exercise-7` directory, this
is where your `PySpark` code to complete the exercise should go.
   
4. Once you have finished the project or want to test run your code,
   run the following command `docker-compose up run` from inside the `Exercises/Exercise-7` directory

#### Problems Statement
There is a folder called `data` in this current directory, `Exercises/Exercise-7`. Inside this
folder there is a `.zip`'d `csv` file, it should remain zipped for the duration of this
exercise. The file is called `hard-drive-2022-01-01-failures.csv.zip`. (This data is free
from https://www.backblaze.com/b2/hard-drive-test-data.html)

Generally the files look like this ...
```
date,serial_number,model,capacity_bytes,failure,smart_1_normalized,smart_1_raw,smart_2_normalized,smart_2_raw,smart_3_normalized,smart_3_raw,smart_4_normalized,smart_4_raw,smart_5_normalized,smart_5_raw,smart_7_normalized,smart_7_raw,smart_8_normalized,smart_8_raw,smart_9_normalized,smart_9_raw,smart_10_normalized,smart_10_raw,smart_11_normalized,smart_11_raw,smart_12_normalized,smart_12_raw,smart_13_normalized,smart_13_raw,smart_15_normalized,smart_15_raw,smart_16_normalized,smart_16_raw,smart_17_normalized,smart_17_raw,smart_18_normalized,smart_18_raw,smart_22_normalized,smart_22_raw,smart_23_normalized,smart_23_raw,smart_24_normalized,smart_24_raw,smart_160_normalized,smart_160_raw,smart_161_normalized,smart_161_raw,smart_163_normalized,smart_163_raw,smart_164_normalized,smart_164_raw,smart_165_normalized,smart_165_raw,smart_166_normalized,smart_166_raw,smart_167_normalized,smart_167_raw,smart_168_normalized,smart_168_raw,smart_169_normalized,smart_169_raw,smart_170_normalized,smart_170_raw,smart_171_normalized,smart_171_raw,smart_172_normalized,smart_172_raw,smart_173_normalized,smart_173_raw,smart_174_normalized,smart_174_raw,smart_175_normalized,smart_175_raw,smart_176_normalized,smart_176_raw,smart_177_normalized,smart_177_raw,smart_178_normalized,smart_178_raw,smart_179_normalized,smart_179_raw,smart_180_normalized,smart_180_raw,smart_181_normalized,smart_181_raw,smart_182_normalized,smart_182_raw,smart_183_normalized,smart_183_raw,smart_184_normalized,smart_184_raw,smart_187_normalized,smart_187_raw,smart_188_normalized,smart_188_raw,smart_189_normalized,smart_189_raw,smart_190_normalized,smart_190_raw,smart_191_normalized,smart_191_raw,smart_192_normalized,smart_192_raw,smart_193_normalized,smart_193_raw,smart_194_normalized,smart_194_raw,smart_195_normalized,smart_195_raw,smart_196_normalized,smart_196_raw,smart_197_normalized,smart_197_raw,smart_198_normalized,smart_198_raw,smart_199_normalized,smart_199_raw,smart_200_normalized,smart_200_raw,smart_201_normalized,smart_201_raw,smart_202_normalized,smart_202_raw,smart_206_normalized,smart_206_raw,smart_210_normalized,smart_210_raw,smart_218_normalized,smart_218_raw,smart_220_normalized,smart_220_raw,smart_222_normalized,smart_222_raw,smart_223_normalized,smart_223_raw,smart_224_normalized,smart_224_raw,smart_225_normalized,smart_225_raw,smart_226_normalized,smart_226_raw,smart_230_normalized,smart_230_raw,smart_231_normalized,smart_231_raw,smart_232_normalized,smart_232_raw,smart_233_normalized,smart_233_raw,smart_234_normalized,smart_234_raw,smart_235_normalized,smart_235_raw,smart_240_normalized,smart_240_raw,smart_241_normalized,smart_241_raw,smart_242_normalized,smart_242_raw,smart_244_normalized,smart_244_raw,smart_245_normalized,smart_245_raw,smart_246_normalized,smart_246_raw,smart_247_normalized,smart_247_raw,smart_248_normalized,smart_248_raw,smart_250_normalized,smart_250_raw,smart_251_normalized,smart_251_raw,smart_252_normalized,smart_252_raw,smart_254_normalized,smart_254_raw,smart_255_normalized,smart_255_raw
2022-01-01,ZLW18P9K,ST14000NM001G,14000519643136,0,73,20467240,,,90,0,100,12,100,0,87,495846641,,,89,9937,100,0,,,100,12,,,,,,,,,100,0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,100,0,100,0,,,66,34,,,100,2,99,2641,34,34,,,,,100,0,100,0,200,0,10
```

Your job is to read this file with `PySpark` and answer the following questions.
Answer each question by adding a new column with the answer. 

1. Add the file name as a column to the DataFrame and call it `source_file`.
2. Pull the `date` located inside the string of the `source_file` column. Final data-type must be 
`date` or `timestamp`, not a `string`. Call the new column `file_date`.
3. Add a new column called `brand`. It will be based on the column `model`. If the
column `model` has a space ... aka ` ` in it, split on that `space`. The value
   found before the space ` ` will be considered the `brand`. If there is no
   space to split on, fill in a value called `unknown` for the `brand`.
   
4. Inspect a column called `capacity_bytes`. Create a secondary DataFrame that
relates `capacity_bytes` to the `model` column, create "buckets" / "rankings" for
   those models with the most capacity to the least. Bring back that 
   data as a column called `storage_ranking` into the main dataset.
   
5. Create a column called `primary_key` that is `hash` of columns that make a record umique
in this dataset.


Note: Your `PySpark` code should be encapsulated inside functions or methods.

Extra Credit: Unit test your PySpark code.
