from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

# TO-DO: create a kafka message schema StructType including the following JSON elements:
# {"accountNumber":"703934969","amount":415.94,"dateAndTime":"Sep 29, 2020, 10:06:23 AM"}

# References:
# https://spark.apache.org/docs/latest/sql-ref-datatypes.html
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.types.StructType.html
# https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.types.StructField.html

schema = StructType([StructField("accountNumber", StringType()),
                     StructField("amount", FloatType()),
                     StructField("dateAndTime", StringType())
                    ])

# TO-DO: create a spark session, with an appropriately named application name
# TO-DO: set the log level to WARN

spark_session = SparkSession.builder.appName("atm_visits").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")

#TO-DO: read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them

raw_stream_df = spark_session\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers","localhost:9092")\
   .option("subscribe","bank-deposits")\
   .option("startingOffsets","earliest")\
   .load()

stream_df = raw_stream_df.selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value")

#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 

stream_df = stream_df.withColumn("value", from_json("value", schema))

# TO-DO: create a temporary streaming view called "BankDeposits" 
# it can later be queried with spark.sql

# note: added .select(col("value.*")) to create individual columns based on the value column.

stream_df.select(col('value.*')).createOrReplaceTempView("BankDeposits")

#TO-DO: using spark.sql, select * from BankDeposits

new_stream_df = spark_session.sql("select * from BankDeposits")

# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
# +-------------+------+--------------------+
# |accountNumber|amount|         dateAndTime|
# +-------------+------+--------------------+
# |    103397629| 800.8|Oct 6, 2020 1:27:...|
# +-------------+------+--------------------+


new_stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()


# to run this application
# /home/workspace/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/bank-deposits.py | tee /home/workspace/spark/logs/bank-deposits.log

# to check input topic
# kafka-console-consumer --topic bank-deposits --bootstrap-server localhost:9092 --from-beginning
