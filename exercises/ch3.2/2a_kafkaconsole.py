from pyspark.sql import SparkSession

#TO-DO: create a Spak Session, and name the app something relevant
#TO-DO: set the log level to WARN
spark_session = SparkSession.builder.appName("kafka_cosole").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")


#TO-DO: read a stream from the kafka topic 'fuel-level', with the bootstrap server localhost:9092, reading from the earliest message

# References:
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.SparkSession.readStream.html#pyspark.sql.SparkSession.readStream
# https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamReader.html
# stream_df is type DataStreamReader

stream_df = spark_session \
   .readStream              \
   .format("kafka")         \
   .option("kafka.bootstrap.servers","localhost:9092") \
   .option("subscribe","fuel-level")              \
   .option("startingOffsets","earliest")              \
   .load()

#TO-DO: cast the key and value columns as strings and select them using a select expression function

# Reference:
# https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.selectExpr.html

stream_df = stream_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
# stream_df = stream_df.selectExpr("cast(key as string) new_col_name1", "cast(value as string) new_col_name2")  # note: you can supply a new column name

#TO-DO: write the dataframe to the console, and keep running indefinitely

# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html --> writestream
# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes

stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()


# How to run:
# /home/workspace/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/kafkaconsole.py | tee /home/workspace/spark/logs/kafkaconsole.log 

