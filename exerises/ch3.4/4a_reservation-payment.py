from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# this is a manually created schema - before Spark 3.0.0, schema inference is not automatic

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

# TO-DO: create a StructType for the Payment schema for the following fields:
# {"reservationId":"9856743232","customerName":"Frank Aristotle","date":"Sep 29, 2020, 10:06:23 AM","amount":"946.88"}

payment_schema = StructType([
    StructField("reservationId", StringType()),
    StructField("customerName", StringType()),
    StructField("date", StringType()),
    StructField("amount", StringType())
])

# TO-DO: create a spark session, with an appropriately named application name
# TO-DO: set the log level to WARN

spark_session = SparkSession.builder.appName('redis-reservation-payment').getOrCreate()
spark_session.sparkContext.setLogLevel('WARN')

#TO-DO: read the redis-server kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
#TO-DO: using the redisMessageSchema StructType, deserialize the JSON from the streaming dataframe 
# TO-DO: create a temporary streaming view called "RedisData" based on the streaming dataframe
# it can later be queried with spark.sql
#TO-DO: using spark.sql, select key, zSetEntries[0].element as redisEvent from RedisData
#TO-DO: from the dataframe use the unbase64 function to select a column called redisEvent with the base64 decoded JSON, and cast it to a string

redis_raw_stream_df = spark_session\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers","localhost:9092")\
   .option("subscribe","redis-server")\
   .option("startingOffsets","earliest")\
   .load()

redis_stream_df = redis_raw_stream_df.selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value")

redis_stream_df\
    .withColumn("value", from_json("value", redisMessageSchema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("redis_vw")

redis_decoded_stream_df = spark_session.sql("select key, zSetEntries[0].element as redis_element from redis_vw")\
    .withColumn("redis_element", unbase64(col("redis_element")).cast("string"))


#TO-DO: repeat this a second time, so now you have two separate dataframes that contain redisEvent data
# Note to myself: Is this really necessary? Isn't it possible to reuse the redis stream?? --> Omit for now!

#TO-DO: using spark.sql select select reservationId, reservationDate from Reservation where reservationDate is not null
#TO-DO: using the reservation StructType, deserialize the JSON from the first redis decoded streaming dataframe, selecting column reservation.* as a temporary view called Reservation 

redis_decoded_stream_df\
    .filter(col("redis_element").contains("reservationDate"))\
    .withColumn("reservation", from_json("redis_element", reservation_schema))\
    .select(col("reservation.*"))\
    .createOrReplaceTempView("reservation_vw")

reservation_stream_df = spark_session.sql("select reservationId, reservationDate from reservation_vw")# where reservationDate is not null")


#TO-DO: using the payment StructType, deserialize the JSON from the second redis decoded streaming dataframe, selecting column payment.* as a temporary view called Payment 
#TO-DO: using spark.sql select reservationId as paymentReservationId, date as paymentDate, amount as paymentAmount from Payment

redis_decoded_stream_df\
    .filter(~col("redis_element").contains("reservationDate"))\
    .withColumn("payment", from_json("redis_element", payment_schema))\
    .select(col("payment.*"))\
    .createOrReplaceTempView("payment_vw")

payment_stream_df = spark_session.sql("select reservationId as reservationId_payment, amount from payment_vw")# where amount is not null")

#TO-DO: join the reservation and payment data using the expression: reservationId=paymentReservationId

reservation_payment_stream_df = reservation_stream_df.join(payment_stream_df, expr("reservationId = reservationId_payment"))

# outer join -- not supported without a watermark in the join keys!
# reservation_payment_stream_df = reservation_stream_df.join(payment_stream_df, expr("reservationId = reservationId_payment"), "leftOuter")

# TO-DO: write the stream to the console, and configure it to run indefinitely
# can you find the reservations who haven't made a payment on their reservation?

reservation_payment_stream_df.writeStream.outputMode("append").format("console").start().awaitTermination()

# to run:
# /home/workspace/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/reservation-payment.py | tee /home/workspace/spark/logs/reservation-payment.log
