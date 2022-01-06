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

# TO-DO: create a StructType for the Customer schema for the following fields:
# {"customerName":"Frank Aristotle","email":"Frank.Aristotle@test.com","phone":"7015551212","birthDay":"1948-01-01","accountNumber":"750271955","location":"Jordan"}

customer_schema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType()),
    StructField("accountNumber", StringType()),
    StructField("location", StringType())
])

# TO-DO: create a spark session, with an appropriately named application name
# TO-DO: set the log level to WARN

spark_session = SparkSession.builder.appName("customer-attributes").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")

# TO-DO: read the redis-server kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
# TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
# TO-DO: using the redisMessageSchema StructType, deserialize the JSON from the streaming dataframe 
# TO-DO: create a temporary streaming view called "RedisData" based on the streaming dataframe
# it can later be queried with spark.sql
# TO-DO: using spark.sql, select key, zSetEntries[0].element as customer from RedisData
# TO-DO: from the dataframe use the unbase64 function to select a column called customer with the base64 decoded JSON, and cast it to a string
# TO-DO: using the customer StructType, deserialize the JSON from the streaming dataframe, selecting column customer.* as a temporary view called Customer 
# TO-DO: using spark.sql select accountNumber, location, birthDay from Customer where birthDay is not null
# TO-DO: select the account number, location, and birth year (using split)


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

customer_stream_df = spark_session.sql("select key, zSetEntries[0].element as customer from redis_vw")

customer_stream_df\
    .withColumn("customer", unbase64(customer_stream_df.customer).cast("string"))\
    .withColumn("customer", from_json("customer", customer_schema))\
    .select(col("customer.*"))\
    .createOrReplaceTempView("customer_vw")

new_customer_stream_df = spark_session.sql("select accountNumber, location, birthDay from customer_vw where birthDay is not null")


# TO-DO: write the stream in JSON format to a kafka topic called customer-attributes, and configure it to run indefinitely, the console output will not show any output. 
#You will need to type /data/kafka/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic customer-attributes --from-beginning to see the JSON data
#
# The data will look like this: {"accountNumber":"288485115","location":"Brazil","birthYear":"1938"}

# write to terminal:
# new_customer_stream_df.select("accountNumber", "location", split(new_customer_stream_df.birthDay, "-").getItem(0).alias("birthYear")).writeStream.outputMode("append").format("console").start().awaitTermination()

# write to kafka:
new_customer_stream_df\
    .select("accountNumber", "location", split(new_customer_stream_df.birthDay, "-").getItem(0).alias("birthYear"))\
    .selectExpr("cast(accountNumber as string) as key", "to_json(struct(*)) as value")\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "customer-attributes")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()



# To run. 
# NOTE: NEED TO START BANKING SIMULATION USING BLUE BUTTON!
# /home/workspace/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/customer-record.py | tee /home/workspace/spark/logs/customer-record.log

# inspect topics in kafka
# kafka-topics --list --zookeeper localhost:2181

# to inspect source data in kafka
# kafka-console-consumer --topic redis-server --bootstrap-server localhost:9092 --from-beginning

# inspect result in kafka
# kafka-console-consumer --topic customer-attributes --bootstrap-server localhost:9092 --from-beginning


# to decode base64
# echo "eyJjdXN0b21lck5hbWUiOiJUcmV2b3IgV3UiLCJlbWFpbCI6IlRyZXZvci5XdUB0ZXN0LmNvbSIsInBob25lIjoiODAxNTU1MTIxMiIsImJpcnRoRGF5IjoiMTk0OS0wMS0wMSIsImFjY291bnROdW1iZXIiOiI5MjQzMTc0NDgiLCJsb2NhdGlvbiI6IklyYXEifQ==" | base64 -d