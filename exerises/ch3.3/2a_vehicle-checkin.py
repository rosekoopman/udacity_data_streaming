from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, IntegerType

# TO-DO: create a Vehicle Status kafka message schema StructType including the following JSON elements:
# {"truckNumber":"5169","destination":"Florida","milesFromShop":505,"odomoterReading":50513}

# TO-DO: create a Checkin Status kafka message schema StructType including the following JSON elements:
# {"reservationId":"1601485848310","locationName":"New Mexico","truckNumber":"3944","status":"In"}

vehicle_status_schema = StructType([StructField("truckNumber", StringType()),
                                    StructField("destination", StringType()),
                                    StructField("milesFromShop", IntegerType()),
                                    StructField("odometerReading", IntegerType())])

vehicle_checkin_schema = StructType([StructField("reservationId", StringType()),
                                     StructField("locationName", StringType()),
                                     StructField("truckNumber", StringType()),
                                     StructField("status", StringType())])

# TO-DO: create a spark session, with an appropriately named application name
#TO-DO: set the log level to WARN

spark_session = SparkSession.builder.appName("vehicle_status_checkin").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")

#TO-DO: read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
# TO-DO: create a temporary streaming view called "VehicleStatus" 
# it can later be queried with spark.sql
#TO-DO: using spark.sql, select truckNumber as statusTruckNumber, destination, milesFromShop, odometerReading from VehicleStatus into a dataframe

vehicle_status_raw_stream_df = spark_session\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers","localhost:9092")\
   .option("subscribe","vehicle-status")\
   .option("startingOffsets","earliest")\
   .load()

vehicle_status_stream_df = vehicle_status_raw_stream_df.selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value")

vehicle_status_stream_df.withColumn("value", from_json("value", vehicle_status_schema)).select(col('value.*')).createOrReplaceTempView("vehicle_status_vw")

#vehicle_status_new_stream_df = spark_session.sql("select truckNumber as truck_number_status, destination, milesFromShop, odometerReading from vehicle_status_vw")
vehicle_status_new_stream_df = spark_session.sql("select * from vehicle_status_vw")




#TO-DO: read the bank-customers kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
# TO-DO: create a temporary streaming view called "VehicleCheckin" 
# it can later be queried with spark.sql
#TO-DO: using spark.sql, select reservationId, locationName, truckNumber as checkinTruckNumber, status from VehicleCheckin into a dataframe


vehicle_checkin_raw_stream_df = spark_session\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers","localhost:9092")\
   .option("subscribe","check-in")\
   .option("startingOffsets","earliest")\
   .load()

vehicle_checkin_stream_df = vehicle_checkin_raw_stream_df.selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value")

vehicle_checkin_stream_df.withColumn("value", from_json("value", vehicle_checkin_schema)).select(col('value.*')).createOrReplaceTempView("vehicle_checkin_vw")

#vehicle_checkin_new_stream_df = spark_session.sql("select truckNumber as truck_number_checkin, reservationId, locationName, status from vehicle_checkin_vw")
vehicle_checkin_new_stream_df = spark_session.sql("select * from vehicle_checkin_vw")

#TO-DO: join the customer dataframe with the deposit dataframe

# Note: either rename the columns or specify from which dataframe you want to take the column! In the later case the same column will occur twice in the resulting join dataframe.

#vehicle_status_checkin_stream_df = vehicle_status_new_stream_df.join(vehicle_checkin_new_stream_df, expr("truck_number_status = truck_number_checkin"))
vehicle_status_checkin_stream_df = vehicle_status_new_stream_df.join(vehicle_checkin_new_stream_df, expr("vehicle_status_vw.truckNumber = vehicle_checkin_vw.truckNumber"))


# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
# +-----------------+------------+-------------+---------------+-------------+------------+------------------+------+
# |statusTruckNumber| destination|milesFromShop|odometerReading|reservationId|locationName|checkinTruckNumber|status|
# +-----------------+------------+-------------+---------------+-------------+------------+------------------+------+
# |             1445|Pennsylvania|          447|         297465|1602364379489|    Michigan|              1445|    In|
# |             1445|     Colardo|          439|         298038|1602364379489|    Michigan|              1445|    In|
# |             1445|    Maryland|          439|         298094|1602364379489|    Michigan|              1445|    In|
# |             1445|       Texas|          439|         298185|1602364379489|    Michigan|              1445|    In|
# |             1445|    Maryland|          439|         298234|1602364379489|    Michigan|              1445|    In|
# |             1445|      Nevada|          438|         298288|1602364379489|    Michigan|              1445|    In|
# |             1445|   Louisiana|          438|         298369|1602364379489|    Michigan|              1445|    In|
# |             1445|       Texas|          438|         298420|1602364379489|    Michigan|              1445|    In|
# |             1445|       Texas|          436|         298471|1602364379489|    Michigan|              1445|    In|
# |             1445|  New Mexico|          436|         298473|1602364379489|    Michigan|              1445|    In|
# |             1445|       Texas|          434|         298492|1602364379489|    Michigan|              1445|    In|
# +-----------------+------------+-------------+---------------+-------------+------------+------------------+------+



vehicle_status_checkin_stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()

# How to run
# /home/workspace/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/vehicle-checkin.py | tee /home/workspace/spark/logs/vehicle-checkin.log