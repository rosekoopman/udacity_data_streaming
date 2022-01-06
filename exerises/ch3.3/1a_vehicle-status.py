from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, IntegerType

# TO-DO: create a kafka message schema StructType including the following JSON elements:
# {"truckNumber":"5169","destination":"Florida","milesFromShop":505,"odomoterReading":50513}

schema = StructType([StructField("truckNumber", StringType()),
                     StructField("destination", StringType()),
                     StructField("milesFromShop", IntegerType()),
                     StructField("odometerReading", IntegerType())
                    ])

# TO-DO: create a spark session, with an appropriately named application name
#TO-DO: set the log level to WARN

spark_session = SparkSession.builder.appName("vehicle_status").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")

#TO-DO: read the vehicle-status kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them

raw_stream_df = spark_session\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers","localhost:9092")\
   .option("subscribe","vehicle-status")\
   .option("startingOffsets","earliest")\
   .load()

stream_df = raw_stream_df.selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value")

#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 

stream_df = stream_df.withColumn("value", from_json("value", schema))

# note: alternatively, to do this and the next step at once
# stream_df.withColumn("value", from_json("value", schema)).select(col('value.*')).createOrReplaceTempView("VehicleStatus")
# note how the output of the withColumn is not captured as it is passed to the select statement at once!

# TO-DO: create a temporary streaming view called "VehicleStatus" 
# it can later be queried with spark.sql

stream_df.select(col('value.*')).createOrReplaceTempView("VehicleStatus")

# note: added .select(col("value.*")) to create individual columns based on the value column.
# if we do not do this select, the output will look like (instead of the desired output as displayed below)
# +----+--------------------+
# | key|               value|
# +----+--------------------+
# |8897|{"truckNumber":"8...|
# |8897|{"truckNumber":"8...|
# |1262|{"truckNumber":"1...|
# |  64|{"truckNumber":"6...|
# +----+--------------------+

#TO-DO: using spark.sql, select * from VehicleStatus

new_stream_df = spark_session.sql("select * from VehicleStatus")

# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
# +-----------+------------+-------------+---------------+
# |truckNumber| destination|milesFromShop|odometerReading|
# +-----------+------------+-------------+---------------+
# |       9974|   Tennessee|          221|         335048|
# |       3575|      Canada|          354|          74000|
# |       1444|      Nevada|          257|         395616|
# |       5540|South Dakota|          856|         176293|
# +-----------+------------+-------------+---------------+




new_stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()


# check input
# kafka-console-consumer --topic vehicle-status --bootstrap-server localhost:9092 --from-beginning

# run this program
# /home/workspace/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/vehicle-status.py | tee /home/workspace/spark/logs/vehicle-status-solution.log 
