from pyspark.sql import SparkSession

# TO-DO: create a spark session, with an appropriately named application name
#TO-DO: set the log level to WARN

spark_session = SparkSession.builder.appName("gear_position").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")

#TO-DO: read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them

raw_stream_df = spark_session\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers","localhost:9092")\
   .option("subscribe","atm-visits")\
   .option("startingOffsets","earliest")\
   .load()

stream_df = raw_stream_df.selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value")

# TO-DO: create a temporary streaming view called "ATMVisits" based on the streaming dataframe

# Reference:
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceTempView.html

stream_df.createOrReplaceTempView("ATMVisits")

# TO-DO query the temporary view with spark.sql, with this query: "select * from ATMVisits"

# Reference:
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.sql.html

new_stream_df = spark_session.sql("select * from ATMVisits")

# TO-DO: write the dataFrame from the last select statement to kafka to the atm-visit-updates topic, on the broker localhost:9092, and configure it to retrieve the earliest messages 

# note1: cast names to key and value as this is what kafka expects!
# note2: checkpointLocation is used by spark workers, used for synchronizing data with spark
# note3: truckid AS key, gear_position AS value. The AS is important here, you cannot ommit. For some reason it is possible
#        to ommit it in line 20, but not here! --> just checked: lecture is incorrect; it also works without AS

new_stream_df\
    .selectExpr("cast(key as string) as key","cast(value as string) as value")\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "atm-visit-updates")\
    .option("checkpointLocation","/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()


# run this script:
# /home/workspace/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/atm-visits.py | tee /home/workspace/spark/logs/atm-visits.log

# check running:
# kafka-console-consumer --topic atm-visit-updates --bootstrap-server localhost:9092 --from-beginning