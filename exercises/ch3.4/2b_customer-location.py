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

# TO-DO: create a StructType for the CustomerLocation schema for the following fields:
# {"accountNumber":"814840107","location":"France"}

customer_location_schema = StructType([
    StructField("accountNumber", StringType()),
    StructField("location", StringType())
])

# TO-DO: create a spark session, with an appropriately named application name
# TO-DO: set the log level to WARN


spark_session = SparkSession.builder.appName("redis-customer-location").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")

# TO-DO: read the redis-server kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
# TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
# TO-DO: using the redisMessageSchema StructType, deserialize the JSON from the streaming dataframe 
# TO-DO: create a temporary streaming view called "RedisData" based on the streaming dataframe
# it can later be queried with spark.sql
# TO-DO: using spark.sql, select key, zSetEntries[0].element as customerLocation from RedisData
# TO-DO: from the dataframe use the unbase64 function to select a column called customerLocation with the base64 decoded JSON, and cast it to a string
# TO-DO: using the customer location StructType, deserialize the JSON from the streaming dataframe, selecting column customer.* as a temporary view called CustomerLocation 
# TO-DO: using spark.sql select * from CustomerLocation

redis_raw_stream_df = spark_session\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers","localhost:9092")\
   .option("subscribe","redis-server")\
   .option("startingOffsets","earliest")\
   .load()

redis_stream_df = redis_raw_stream_df.selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value")

redis_stream_df.withColumn("value", from_json("value", redisMessageSchema)).select(col("value.*")).createOrReplaceTempView("redis_vw")

customer_location_stream_df = spark_session.sql("select key, zSetEntries[0].element as customer_location from redis_vw")

customer_location_stream_df\
    .withColumn("customer_location", unbase64(customer_location_stream_df.customer_location).cast("string"))\
    .withColumn("customer_location", from_json("customer_location", customer_location_schema))\
    .select(col("customer_location.*"))\
    .createOrReplaceTempView("customer_location_vw")

new_customer_location_stream_df = spark_session.sql("select * from customer_location_vw")




# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
# +-------------+---------+
# |accountNumber| location|
# +-------------+---------+
# |         null|     null|
# |     93618942|  Nigeria|
# |     55324832|   Canada|
# |     81128888|    Ghana|
# |    440861314|  Alabama|
# |    287931751|  Georgia|
# |    413752943|     Togo|
# |     93618942|Argentina|
# +-------------+---------+


new_customer_location_stream_df.writeStream.outputMode("append").format("console").start().awaitTermination()

# to run
# /home/workspace/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/customer-location.py | tee /home/workspace/spark/logs/customer-location.log
