from pyspark.sql import SparkSession

# TO-DO: create a variable with the absolute path to the text file
# /home/workspace/Test.txt

filename = "/home/workspace/Test.txt"

# TO-DO: create a Spark session
# TO-DO: set the log level to WARN
spark_session = SparkSession.builder.appName("hello_spark").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")

# References
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.builder.appName.html
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.builder.getOrCreate.html
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.setLogLevel.html

# TO-DO: using the Spark session variable, call the appropriate
# function referencing the text file path to read the text file 


spark_df = spark_session.read.text(filename).cache()

# Note: to see the contents of the spark dataframe
# spark_df.show()
# This is comparable to pandas_df.head()

# Reference:
# https://spark.apache.org/docs/latest/sql-data-sources-text.html

# TO-DO: call the appropriate function to filter the data containing
# the letter 'a', and then count the rows that were found
# TO-DO: call the appropriate function to filter the data containing
# the letter 'b', and then count the rows that were found
# TO-DO: print the count for letter 'd' and letter 's'

# note: value is the name of the column in which the data is stored!
na = spark_df.filter(spark_df.value.contains("a")).count()
nb = spark_df.filter(spark_df.value.contains("b")).count()
nd = spark_df.filter(spark_df.value.contains("d")).count()
ns = spark_df.filter(spark_df.value.contains("s")).count()

print(f"\n###\n\na : {na}\nb : {nb}\nd : {nd}\ns : {ns}\n\n###\n")

# References:
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.filter.html
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.contains.html
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.count.html


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