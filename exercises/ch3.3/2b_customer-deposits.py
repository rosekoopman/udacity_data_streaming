from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, FloatType, BooleanType, ArrayType, DateType

# TO-DO: create deposit a kafka message schema StructType including the following JSON elements:
# {"accountNumber":"703934969","amount":415.94,"dateAndTime":"Sep 29, 2020, 10:06:23 AM"}
# Cast the amount as a FloatType

# TO-DO: create a customer kafka message schema StructType including the following JSON elements:
# {"customerName":"Trevor Anandh","email":"Trevor.Anandh@test.com","phone":"1015551212","birthDay":"1962-01-01","accountNumber":"45204068","location":"Togo"}

deposit_schema = StructType([StructField("accountNumber", StringType()),
                             StructField("dateAndTime", StringType()),   
                             StructField("amount", FloatType())
                            ])

customer_schema = StructType([StructField("customerName", StringType()),
                             StructField("email", StringType()),
                             StructField("phone", StringType()),
                             StructField("birthDay", StringType()),
                             StructField("accountNumber", StringType()),
                             StructField("location", StringType())
                             ])


# TO-DO: create a spark session, with an appropriately named application name
# TO-DO: set the log level to WARN

spark_session = SparkSession.builder.appName("bank").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")

#TO-DO: read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
# TO-DO: create a temporary streaming view called "BankDeposits" 
# it can later be queried with spark.sql
#TO-DO: using spark.sql, select * from BankDeposits where amount > 200.00 into a dataframe

deposit_raw_stream_df = spark_session\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers","localhost:9092")\
   .option("subscribe","bank-deposits")\
   .option("startingOffsets","earliest")\
   .load()

deposit_stream_df = deposit_raw_stream_df.selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value")

deposit_stream_df.withColumn("value", from_json("value", deposit_schema)).select(col('value.*')).createOrReplaceTempView("deposit_vw")

deposit_new_stream_df = spark_session.sql("select accountNumber as account_number_deposit, dateAndTime, amount from deposit_vw where amount > 200")


#TO-DO: read the bank-customers kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
# TO-DO: create a temporary streaming view called "BankCustomers" 
# it can later be queried with spark.sql
#TO-DO: using spark.sql, select customerName, accountNumber as customerNumber from BankCustomers into a dataframe

customer_raw_stream_df = spark_session\
   .readStream\
   .format("kafka")\
   .option("kafka.bootstrap.servers","localhost:9092")\
   .option("subscribe","bank-customers")\
   .option("startingOffsets","earliest")\
   .load()

customer_stream_df = customer_raw_stream_df.selectExpr("CAST(key AS STRING) key", "CAST(value AS STRING) value")

customer_stream_df.withColumn("value", from_json("value", customer_schema)).select(col('value.*')).createOrReplaceTempView("customer_vw")

customer_new_stream_df = spark_session.sql("select accountNumber as account_number_customer, customerName from customer_vw")

#TO-DO: join the customer dataframe with the deposit dataframe

customer_deposit_df = deposit_new_stream_df.join(customer_new_stream_df, expr("account_number_deposit = account_number_customer"))


# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
#. +-------------+------+--------------------+------------+--------------+
#. |accountNumber|amount|         dateAndTime|customerName|customerNumber|
#. +-------------+------+--------------------+------------+--------------+
#. |    335115395|142.17|Oct 6, 2020 1:59:...| Jacob Doshi|     335115395|
#. |    335115395| 41.52|Oct 6, 2020 2:00:...| Jacob Doshi|     335115395|
#. |    335115395| 261.8|Oct 6, 2020 2:01:...| Jacob Doshi|     335115395|
#. +-------------+------+--------------------+------------+--------------+



customer_deposit_df.writeStream.outputMode("append").format("console").start().awaitTermination()

# to run
# /home/workspace/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 /home/workspace/customer-deposits.py | tee /home/workspace/spark/logs/customer-deposits.log