from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)

# TO-DO: create a StructType for the Reservation schema for the following fields:
# {"reservationId":"814840107","customerName":"Jim Harris", "truckNumber":"15867", "reservationDate":"Sep 29, 2020, 10:06:23 AM"}


reservation_schema = StructType([
    StructField("reservationId", StringType()),
    StructField("customerName", StringType()),
    StructField("truckNumber", StringType()),
    StructField("reservationDate", StringType())
])

# TO-DO: create a spark session, with an appropriately named application name
# TO-DO: set the log level to WARN

spark_session = SparkSession.builder.appName("redis-reservation").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")

#TO-DO: read the redis-server kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible   


redis_raw_stream_df = spark_session\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers","localhost:9092")\
   .option("subscribe","redis-server")\
   .option("startingOffsets","earliest")\
   .load()

#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them

redis_stream_df = redis_raw_stream_df.selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value")

#TO-DO: using the redisMessageSchema StructType, deserialize the JSON from the streaming dataframe 
# TO-DO: create a temporary streaming view called "RedisData" based on the streaming dataframe
# it can later be queried with spark.sql

redis_stream_df.withColumn("value", from_json("value", redisMessageSchema)).select(col("value.*")).createOrReplaceTempView("RedisData")


#TO-DO: using spark.sql, select key, zSetEntries[0].element as reservation from RedisData

new_redis_stream_df = spark_session.sql("select key, zSetEntries[0].element as reservation from RedisData")

#TO-DO: from the dataframe use the unbase64 function to select a column called reservation with the base64 decoded JSON, and cast it to a string

new_redis_string_stream_df = new_redis_stream_df.withColumn("reservation", unbase64(new_redis_stream_df.reservation).cast("string"))

#TO-DO: using the truck reservation StructType, deserialize the JSON from the streaming dataframe, selecting column reservation.* as a temporary view called TruckReservation 

reservation_stream_df = new_redis_string_stream_df\
                            .withColumn("reservation", from_json("reservation", reservation_schema)).select(col("reservation.*"))\
                            .createOrReplaceTempView("TruckReservation")

#TO-DO: using spark.sql select * from TruckReservation where reservationDate is not null

new_reservation_stream_df = spark_session.sql("select * from TruckReservation where reservationDate is not null")

# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:

new_reservation_stream_df.writeStream.outputMode("append").format("console").start().awaitTermination()


# to run
# /home/workspace/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/reservation-base64.py | tee /home/workspace/spark/logs/reservation-base64.logs