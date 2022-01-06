from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, IntegerType, FloatType

# TO-DO: create bank withdrawals kafka message schema StructType including the following JSON elements:
#  {"accountNumber":"703934969","amount":625.8,"dateAndTime":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682}

# TO-DO: create an atm withdrawals kafka message schema StructType including the following JSON elements:
# {"transactionDate":"Sep 29, 2020, 10:06:23 AM","transactionId":1601395583682,"atmLocation":"Thailand"}


# Note: even though transactionId is of type Integer, I need to set it to string in order for the from_json to work! If
# I do not set it to string, it will return only NULL values! Not sure why this is the case. Using the kafka-console-consumer
# I confirmed that the dtype is integer and not string! Maybe it has something to do with the fact that transactionId is also
# used as key?

bank_schema = StructType([
    StructField("accountNumber", StringType()),
    StructField("amount", FloatType()),
    StructField("dateAndTime", StringType()),
    StructField("transactionId", StringType())    
])


atm_schema = StructType([
    StructField("transactionDate", StringType()),
    StructField("transactionId", StringType()),
    StructField("atmLocation", StringType())
])

# TO-DO: create a spark session, with an appropriately named application name
# TO-DO: set the log level to WARN

spark_session = SparkSession.builder.appName("withdrawal-info").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")

#TO-DO: read the bank-withdrawals kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
# TO-DO: create a temporary streaming view called "BankWithdrawals" 
# it can later be queried with spark.sql
#TO-DO: using spark.sql, select * from BankWithdrawals into a dataframe

bank_raw_stream_df = spark_session\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "bank-withdrawals")\
    .option("startingOffsets", "earliest")\
    .load()

bank_stream_df = bank_raw_stream_df.selectExpr("cast(key as string) key", "cast(value as string) value")

bank_new_stream_df = bank_stream_df.withColumn("value", from_json("value", bank_schema)).select(col("value.*"))


#TO-DO: read the atm-withdrawals kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
# TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
# TO-DO: create a temporary streaming view called "AtmWithdrawals" 
# it can later be queried with spark.sql
#TO-DO: using spark.sql, select * from AtmWithdrawals into a dataframe

atm_raw_stream_df = spark_session\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "atm-withdrawals")\
    .option("startingOffsets", "earliest")\
    .load()

atm_stream_df = atm_raw_stream_df.selectExpr("cast(key as string) key", "cast(value as string) value")

atm_new_stream_df = atm_stream_df.withColumn("value", from_json("value", atm_schema)).select(col("value.*"))

#TO-DO: join the atm withdrawals dataframe with the bank withdrawals dataframe

bank_atm_stream_df = bank_new_stream_df.join(atm_new_stream_df, bank_new_stream_df["transactionId"]==atm_new_stream_df["transactionId"])

# TO-DO: write the stream to the kafka in a topic called withdrawals-location, and configure it to run indefinitely, the console will not output anything. You will want to attach to the topic using the kafka-console-consumer inside another terminal, it will look something like this:

# # write to console to check output
# bank_atm_stream_df.writeStream\
#     .outputMode("append")\
#     .format("console")\
#     .start()\
#     .awaitTermination()


# Note: now I have two transactionId columns! This makes it impossible to select the column transactionId as key (becayse ambiguous column error). I will use another field as key to mitigate.
# --> this is why it is useful to create the view with the renamed column!!!

bank_atm_stream_df.selectExpr("cast(accountNumber as string) as key", "to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "withdrawals-location")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()


# {"accountNumber":"862939503","amount":"844.8","dateAndTime":"Oct 7, 2020 12:33:34 AM","transactionId":"1602030814320","transactionDate":"Oct 7, 2020 12:33:34 AM","atmTransactionId":"1602030814320","atmLocation":"Ukraine"}


# how to run:
# /home/workspace/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/bank-withdrawals.py | tee /home/workspace/spark/logs/bank-withdrawals.log
