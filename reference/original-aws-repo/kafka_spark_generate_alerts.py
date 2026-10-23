from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, struct, when, lit, concat_ws, udf

spark=SparkSession.builder.appName("Alerting System").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

in_schema = StructType([
       StructField("patientId", StringType(), False),
       StructField("heartBeat", FloatType(), True),
       StructField("systolicBP", FloatType(), True),
       StructField("diastolicBP", FloatType(), True),
       StructField("temperature", FloatType(), True),
       StructField("respirationRate", FloatType(), True),
       StructField("spO2", FloatType(), True),
       StructField("timestamp", TimestampType(), False)
])

threshold_schema = StructType([
       StructField("Metric", StringType(), False),
       StructField("Normal_low_val", FloatType(), False),
       StructField("Normal_high_val", FloatType(), False)
])

threshold_df = spark.read.csv("threshold_values.csv", schema=threshold_schema).cache()

vital_stream_df = spark \
       .readStream \
       .format("kafka") \
       .option("kafka.bootstrap.servers", "localhost:9092") \
       .option("subscribe", "Patients-Vitals-Info") \
       .option("startingOffsets", "earliest")     \
       .load() \
       .selectExpr("CAST(value AS STRING)") \
       .select(from_json(col("value"), in_schema).alias("data")) \
       .select("data.*")

condition_df = vital_stream_df.crossJoin(threshold_df)

# Define alert messages based on violations
# alert_messages = [
#     "Low Heart Rate than Normal",
#     "High Heart Rate than Normal",
#     "Low Systolic BP than Normal",
#     "High Systolic BP than Normal",
#     "Low Diastolic BP than Normal",
#     "High Diastolic BP than Normal",
#     "Low Temperature than Normal",
#     "High Temperature than Normal",
#     "Low Respiration Rate than Normal",
#     "High Respiration Rate than Normal",
#     "Low spO2 than Normal",
#     "High spO2 than Normal"
# ]

# Add a column 'alert_message' based on the detected violation
violations_df = condition_df.withColumn("alert_message", 
        when((col("Metric") == "heartBeat") & (col("heartBeat") < col('Normal_low_val')), lit("Low Heart Rate than Normal")) 
        .when((col("Metric") == "heartBeat") & (col("heartBeat") > col('Normal_high_val')), lit("High Heart Rate than Normal"))      
        .when((col("Metric") == "systolicBP") & (col("systolicBP") < col('Normal_low_val')), lit("Low Systolic BP than Normal"))     
        .when((col("Metric") == "systolicBP") & (col("systolicBP") > col('Normal_high_val')), lit("High Systolic BP than Normal"))   
        .when((col("Metric") == "diastolicBP") & (col("diastolicBP") < col('Normal_low_val')), lit("Low Diastolic BP than Normal"))  
        .when((col("Metric") == "diastolicBP") & (col("diastolicBP") > col('Normal_high_val')), lit("High Diastolic BP than Normal"))
        .when((col("Metric") == "temperature") & (col("temperature") < col('Normal_low_val')), lit("Low Temperature than Normal"))   
        .when((col("Metric") == "temperature") & (col("temperature") > col('Normal_high_val')), lit("High Temperature than Normal")) 
        .when((col("Metric") == "respirationRate") & (col("respirationRate") < col('Normal_low_val')), lit("Low Respiration Rate than Normal"))    
        .when((col("Metric") == "respirationRate") & (col("respirationRate") > col('Normal_high_val')), lit("High Respiration Rate than Normal"))  
        .when((col("Metric") == "spO2") & (col("spO2") < col('Normal_low_val')), lit("Low spO2 than Normal"))
        .when((col("Metric") == "spO2") & (col("spO2") > col('Normal_high_val')), lit("High spO2 than Normal"))
        .otherwise("No Alert")
)

violations_df = violations_df.filter(col("alert_message") != "No Alert").select("patientId", "heartBeat", "systolicBP", "diastolicBP", 
                                                                                "temperature", "respirationRate", "spO2", "alert_message")

info_df = spark.read.csv("patient_vitals_CSV/patient_info.csv")

violations_info_df = violations_df.join(info_df, on="patientId", how='left') \
       .select("patientId", "heartBeat", "systolicBP", "diastolicBP", 
               "temperature", "respirationRate", "spO2", "alert_message",
               "patient_name", "phone_number", "age", "admitted_ward", "address")

query = violations_info_df.writeStream \
    .format("csv")  \
    .outputMode("append") \
    .option("path", "alerts_csv")    \
    .option("checkpointLocation", "./alert_checkpoint/") \
    .trigger(once=True) \
    .start()

query.awaitTermination()

query_kafka = violations_info_df.selectExpr("to_json(struct(*)) AS value")

query_kafka.writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "Alerts-topic") \
    .option("startingOffsets", "earliest") \
    .option("checkpointLocation", "./alerts_checkpoint/alerts/") \
    .start() \
    .awaitTermination()

