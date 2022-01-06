from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, split
import pyspark.sql.functions as F

# 4 things in this script:
# -1- count the number of lines containing 'a' (na)
# -2- count the total number of 'a' (NA)
# -3- count the number of 'a' per line (n_a)
# -4- count the total number of 'a' by summing the counts in each row

# Global variables to keep track of the number of occurences of 'a' and 'b'
NA = 0
NB = 0
Ns = 0 

# TO-DO: create a variable with the absolute path to the text file
# /home/workspace/Test.txt

filename = "/home/workspace/Test.txt"

# TO-DO: create a Spark session
# TO-DO: set the log level to WARN

spark_session = SparkSession.builder.appName("hello_spark").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")

# TO-DO: Define a python function that accepts row as in an input, and increments the total number of times the letter 'a' has been encountered (including in this row)    

# TO-DO: Define a python function that accepts row as in an input, and increments the total number of times the letter 'b' has been encountered (including in this row)


def count_a(row):
    global NA
    NA = NA + row.value.count('a')
    print(f"NA = {NA}")
    return

def count_b(row):
    global NB
    NB = NB + row.value.count('b')
    print(f"NB = {NB}")
    return

def string_count(row, my_string):
    global Ns
    Ns = Ns + row.value.count(my_string)
    print(f"N_{my_string} = {Ns}")
    return

# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file 

spark_df = spark_session.read.text(filename).cache()

# TO-DO: call the appropriate function to filter the data containing
# the letter 'a', and then count the rows that were found

# TO-DO: call the appropriate function to filter the data containing
# the letter 'b', and then count the rows that were found

# -1- count the number of lines containing 'a' (na)
# note: value is the name of the column in which the data is stored!
# count the number of lines with 'a' and 'b'
na = spark_df.filter(spark_df.value.contains("a")).count()
nb = spark_df.filter(spark_df.value.contains("b")).count()
print(f'na = {na}, nb = {nb}')

# TO-DO: print the count for letter 'a' and letter 'b'

# Reference:
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.foreach.html

# -2- count the total number of 'a' (NA)
# count the total number of occurences for 'a' and 'b'
spark_df.foreach(count_a)
spark_df.foreach(count_b)

spark_df.foreach(lambda x: string_count(x, 'a')) 
spark_df.foreach(lambda x: string_count(x, 'b')) 

# -3- count the number of 'a' per line (n_a)
# store the count of a/b in a new column n_a / n_b
# split on 'a' and then measure the size of the array
spark_df = spark_df.withColumn('n_a', size(split(col("value"), "a")) - 1)\
.withColumn('n_b', size(split(col("value"), "b")) - 1)

spark_df.show()

# -4- count the total number of 'a' by summing the counts in each row
# supposedly slow
a_tot = spark_df.agg(F.sum("n_a")).collect()[0][0]
b_tot = spark_df.agg(F.sum("n_b")).collect()[0][0]

print(f'Totals: a={a_tot}, b={b_tot}')

# supposedly much faster -- column is selected by index: x[1] for a, x[2] for b
a_tot_fast = spark_df.rdd.map(lambda x: (1,x[1])).reduceByKey(lambda x,y: x + y).collect()[0][1]
b_tot_fast = spark_df.rdd.map(lambda x: (1,x[2])).reduceByKey(lambda x,y: x + y).collect()[0][1]

print(f'Totals: a={a_tot_fast}, b={b_tot_fast}')


# TO-DO: stop the spark application

spark_session.stop()

### How to startup spark on the workspace:
#
# /home/workspace/spark/sbin/start-master.sh --> copy url to logs
# check logs for spark master uri: cat  /home/workspace/spark/logs/spark--org.apache.spark.deploy.master.Master-1-d37dee3f2450.out --> 21/12/27 12:22:17 INFO Master: Starting Spark master at spark://d37dee3f2450:7077
# /home/workspace/spark/sbin/start-slaves.sh spark://d37dee3f2450:7077
# Check if all is up: ps -ef --> you should see something with java (and spark?)

### To run this application
#
# /home/workspace/spark/bin/spark-submit hellospark.py