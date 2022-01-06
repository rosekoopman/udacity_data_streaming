from pyspark.sql import SparkSession

#TO-DO: create a Spak Session, and name the app something relevant

#TO-DO: set the log level to WARN

spark_session = SparkSession.builder.appName("kafka_console").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")


#TO-DO: read a stream from the kafka topic 'balance-updates', with the bootstrap server localhost:9092, reading from the earliest message

stream_df = spark_session \
   .readStream              \
   .format("kafka")         \
   .option("kafka.bootstrap.servers","localhost:9092") \
   .option("subscribe","balance-updates")              \
   .option("startingOffsets","earliest")              \
   .load()

#TO-DO: cast the key and value columns as strings and select them using a select expression function

stream_df = stream_df.selectExpr("CAST(key AS STRING) key_col", "CAST(value AS STRING) Balance")

#TO-DO: write the dataframe to the console, and keep running indefinitely

stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()


# /home/workspace/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/kafkaconsole.py | tee /home/workspace/spark/logs/kafkaconsole.log 


# kafka-console-consumer --bootstrap-server localhost:9092 --topic balance-updates