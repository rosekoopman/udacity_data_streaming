from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

# define schema for STEDI events
stedi_event_schema = StructType([
    StructField("customer", StringType()),
    StructField("score", FloatType()),
    StructField("riskDate", StringType())
])

# start spark session
spark_session = SparkSession.builder.appName('redis-customer-location').getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")

# read STEDI events from kafka, unpack the json and select the relevant columns
stedi_raw_stream_df = spark_session\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers","localhost:9092")\
   .option("subscribe","stedi-events")\
   .option("startingOffsets","earliest")\
   .load()

stedi_stream_df = stedi_raw_stream_df.selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value")

stedi_stream_df\
    .withColumn("value", from_json("value", stedi_event_schema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("stedi_vw")

customer_risk_stream_df = spark_session.sql("select customer, score from stedi_vw")

# sink stream to the console
customer_risk_stream_df.writeStream.outputMode("append").format("console").start().awaitTermination()

# To run:
# /home/workspace/submit-event-kafkastreaming.sh


###### Instructions ######

# TO-DO: using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
                                   
# TO-DO: cast the value column in the streaming dataframe as a STRING 

# TO-DO: parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
# TO-DO: execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
# TO-DO: sink the customerRiskStreamingDF dataframe to the console in append mode
# 
# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct 