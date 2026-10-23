from kafka import KafkaProducer
import numpy as np
import scipy.stats as stats
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
# from pyspark.sql.functions import to_json, struct
import json
import random
import time
from datetime import datetime, timedelta
from pytz import timezone

tz = timezone('EST')

def generate_patient_vitals(id):
    t=2.37
    n=1
    # Simulate random values for heart rate (bpm)
    heart_rate = int(np.round(stats.truncnorm.rvs(-t, t, 72, 12, n), 2)[0])
    
    # Simulate random values for blood pressure (mmHg)
    systolic_bp = int(np.round(stats.truncnorm.rvs(-t, t, 115, 12, n), 2)[0])
    diastolic_bp = int(np.round(stats.truncnorm.rvs(-t, t, 75, 10, n), 2)[0])

    # Simulate random values for temperature (°C)
    temperature = np.round(stats.truncnorm.rvs(-t, t, 97, 1.4, n), 2)[0]

    # Simulate random values for respiration rate (breaths per minute)
    respiration_rate = int(np.round(stats.truncnorm.rvs(-t, t, 17, 3, n), 2)[0])

    # Simulate random values for SpO2 (%)
    spo2 = np.round(stats.truncnorm.rvs(-t, t, 97, 1.5, n), 2)
    spo2[spo2 > 100] = 100
    spo2=spo2[0]

    date_time= datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

    return {
        'Patient ID': id,
        'Heart Rate': heart_rate,
        'Systolic BP': systolic_bp,
        'Diastolic BP': diastolic_bp,
        'Temperature': temperature,
        'Respiration Rate': respiration_rate,
        'SpO2': spo2,
        'Datetime': date_time
    }


# # def create_spark_session():
# #     return SparkSession.builder.appName("VitalsProducer").getOrCreate()

# # def produce_streaming_data(spark, kafka_bootstrap_servers, kafka_topic):
# #     schema = StructType([
# #         StructField("patientId", IntegerType(), True),
# #         StructField("heartBeat", FloatType(), True),
# #         StructField("systolicBP", FloatType(), True),
# #         StructField("diastolicBP", FloatType(), True),
# #         StructField("temperature", FloatType(), True),
# #         StructField("respirationRate", FloatType(), True),
# #         StructField("spO2", FloatType(), True)
# #     ])

# #     def generate_vitals_udf():
# #         vitals = generate_patient_vitals()
# #         return json.dumps(vitals)

# #     # Use the user-defined function (UDF) to generate vitals and create a DataFrame
# #     spark.udf.register("generate_vitals_udf", generate_vitals_udf)
# #     vitals_df = spark.readStream.format("rate").load() \
# #         .withColumn("vitals", to_json(struct(schema.names))) \
# #         .selectExpr("generate_vitals_udf() as vitals")

# #     # vitals_df = spark.readStream.format("rate").load()  # Creating a dummy DataFrame with a rate source for simulation
# #     # vitals_df = vitals_df.withColumn("vitals", to_json(struct(schema.names))).select("vitals")

# #     # Write the DataFrame to Kafka topic
# #     query = vitals_df \
# #         .writeStream \
# #         .outputMode("append") \
# #         .format("kafka") \
# #         .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
# #         .option("topic", kafka_topic) \
# #         .option("checkpointLocation", "checkpoint") \
# #         .start()

# #     query.awaitTermination()

# # if __name__ == "__main__":
# #     spark = create_spark_session()
# #     kafka_bootstrap_servers = "localhost:9092"
# #     kafka_topic = "patient_vitals"

# #     produce_streaming_data(spark, kafka_bootstrap_servers, kafka_topic)

# #---------------------------Using Kafka-python-------------------------------------
producer = KafkaProducer(
    # docker
    #bootstrap_servers='kafka:9093',
    # Cluster
    bootstrap_servers = "localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_vitals():
    while True:
        #vitals = []
        for id in range(1,50001):  # Run for 10 seconds
            producer.send('patientvitals', generate_patient_vitals(id))
        time.sleep(10)

if __name__ == "__main__":
    send_vitals()