from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql.functions import col, expr

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaProducer").getOrCreate()

# Define the schema for the CSV file
schema = StructType([
    StructField("patientId", IntegerType(), True),
    StructField("heartBeat", FloatType(), True),
    StructField("systolicBP", FloatType(), True),
    StructField("diastolicBP", FloatType(), True),
    StructField("temperature", FloatType(), True),
    StructField("respirationRate", FloatType(), True),
    StructField("spO2", FloatType(), True)
])

# Read data from the CSV file
df_stream = spark.readStream \
    .format("csv") \
    .option("header", True) \
    .schema(schema) \
    .option("path", "patient_vitals_CSV") \
    .load()

df_stream = df_stream.withColumn("timestamp", expr("current_timestamp()"))

query = df_stream.selectExpr("to_json(struct(*)) AS value")

# Write dataframe to S3 (To be changed to S3 link. Currently local path for testing)

df_stream.writeStream   \
    .format("csv")  \
    .outputMode("append") \
    .option("path", "result")    \
    .option("checkpointLocation", "./batch_checkpoint/") \
    .trigger(once=True) \
    .start()

# Write the transformed data to a Kafka topic
query.writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "Patients-Vitals-Info") \
    .option("startingOffsets", "earliest") \
    .option("checkpointLocation", "./kafka_checkpoint/produce_patient_vitals/") \
    .start() \
    .awaitTermination()
    
# The following code is if we want to add an artificial delay between rows of the dataframe to have different timestamps

# # Define the artificial delay between batches (in seconds)
# batch_delay = 5  # Adjust as needed

# # Define the custom function with artificial delay
# def process_batch(batch_df, batch_id):
#     # Introduce an artificial delay
#     time.sleep(batch_delay)

#     # Write to Kafka directly
#     batch_df.selectExpr("CAST(null AS STRING) as key", "to_json(struct(*)) AS value") \
#         .write \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", kafka_params["kafka.bootstrap.servers"]) \
#         .option("topic", kafka_topic) \
#         .save()

# # Define the streaming query
# query = csv_stream_df.writeStream \
#     .trigger(processingTime="10 seconds")  # Adjust the overall processing time if needed
#     .outputMode("append") \
#     .foreachBatch(process_batch) \
#     .start()

# Stop the SparkSession
spark.stop()
