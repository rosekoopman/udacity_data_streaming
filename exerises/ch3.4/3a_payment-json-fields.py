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

spark_session = SparkSession.builder.appName("redis-payment").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")


#TO-DO: read the redis-server kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
#TO-DO: using the redisMessageSchema StructType, deserialize the JSON from the streaming dataframe 
# TO-DO: create a temporary streaming view called "RedisData" based on the streaming dataframe
# it can later be queried with spark.sql
#TO-DO: using spark.sql, select key, zSetEntries[0].element as payment from RedisData
#TO-DO: from the dataframe use the unbase64 function to select a column called payment with the base64 decoded JSON, and cast it to a string
#TO-DO: using the payment StructType, deserialize the JSON from the streaming dataframe, selecting column customer.* as a temporary view called Customer 
#TO-DO: using spark.sql select reservationId, amount,  from Payment

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

payment_stream_df = spark_session.sql("select key, zSetEntries[0].element as payment from redis_vw")

payment_stream_df\
    .withColumn("payment", unbase64(payment_stream_df.payment).cast("string"))\
    .withColumn("payment", from_json("payment", payment_schema))\
    .select(col("payment.*"))\
    .createOrReplaceTempView("payment_vw")

new_payment_stream_df = spark_session.sql("select reservationId, amount from payment_vw where amount is not null")


# TO-DO: write the stream in JSON format to a kafka topic called payment-json, and configure it to run indefinitely, the console output will not show any output. 
#You will need to type /data/kafka/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic customer-attributes --from-beginning to see the JSON data
#
# The data will look like this: {"reservationId":"9856743232","amount":"946.88"}

new_payment_stream_df\
    .selectExpr("cast(reservationId as string) as key", "to_json(struct(*)) as value")\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("topic","payment")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()
    

# to run
# /home/workspace/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/payment-json-fields.py | tee /home/workspace/spark/logs/payment.log

# to check the data in kafka
# kafka-console-consumer --topic payment --bootstrap-server localhost:9092 --from-beginning