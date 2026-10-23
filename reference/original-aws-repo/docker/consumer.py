# consumer.py
#Usage
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 consumer.py [public_DNS]

#from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, expr
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import col
import functools
import json
import os
import boto3
import pandas as pd
import sys
from datetime import datetime

redshift = boto3.client("redshift-data", region_name="us-east-1")
sns = boto3.client('sns',region_name='us-east-1')
# secret_arn = 'arn:aws:iam::903187628715:role/myRedshiftRole' #redshift
snsTopicArn = 'arn:aws:sns:us-east-1:903187628715:patient_vital_alert' #sns
s3 = boto3.client('s3')

s3_bucket = 'patient-vital-monitoring-system'
stream_csv_path = 'patient_vitals.csv'
patient_csv_path = 'patients.csv'
alerts_csv_path = 'alerts.csv'

def create_csv_if_not_exists(bucket, key, columns):
    try:
        # Check if the file exists
        s3.head_object(Bucket=bucket, Key=key)
    except Exception as e:
        # If the file does not exist, create it with headers
        if '404' in str(e):
            empty_df = pd.DataFrame(columns=columns)
            s3.put_object(Body=empty_df.to_csv(index=False), Bucket=bucket, Key=key)

# # Create CSV files if not present
# create_csv_if_not_exists(s3_bucket, stream_csv_path, ["patient_id", "heart_rate", "systolic_bp", "diastolic_bp", "temperature", "respiration_rate", "spo2", "date_time"])
create_csv_if_not_exists(s3_bucket, alerts_csv_path, ["patient_id", "alert_metric", "value", "threshold_range", "alert_timestamp"])


threshold_values = {
    'Heart Rate': (50,100),
    'Systolic BP': (50, 140),
    'Diastolic BP': (50, 90),
    'Temperature': (94,100),
    'Respiration Rate': (8,20),
    'SpO2': (94.5,101)
}

if __name__ == "__main__":
    spark = SparkSession.builder.appName("VitalsAlerts")\
        .getOrCreate()
    # docker 
    # kafka_bootstrap_servers = "kafka:9093"
    # Cluster
    kafka_bootstrap_servers = f"{sys.argv[1]}:9092"
    kafka_topic = "patientvitals"
    spark.sparkContext.setLogLevel('WARN')
    # Read data from Kafka using the readStream API
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest")\
        .load()
    
    schema = StructType([
    StructField("Patient ID", IntegerType(), True),
    StructField("Heart Rate", IntegerType(), True),
    StructField("Systolic BP", IntegerType(), True),
    StructField("Diastolic BP", IntegerType(), True),
    StructField("Temperature", FloatType(), True),
    StructField("Respiration Rate", IntegerType(), True),
    StructField("SpO2", FloatType(), True),
    StructField("Datetime", TimestampType(), True)
    ])

    #value_df = kafka_stream_df.selectExpr("CAST(value AS STRING)")
    json_df = kafka_stream_df.select(from_json(col("value").cast("string"), schema).alias("value")).select("value.*")

    # Write the streaming result to the console
    # query = json_df.writeStream.outputMode("append").format("console").start()

    # query.awaitTermination()

    def load_stream(batch_df):
        # for row in records.toLocalIterator():
            # insert the row the main table
            # redshift_query = f"""
            # INSERT INTO public.patient_vitals
            # (patient_id, heart_rate, systolic_bp, diastolic_bp, temperature, respiration_rate, spo2)
            # VALUES
            # ({row["Patient ID"]}, {row["Heart Rate"]}, {row["Systolic BP"]}, {row["Diastolic BP"]}, {row["Temperature"]}, {row["Respiration Rate"]}, {row["SpO2"]})"""
            # #redshift.execute(redshift_query)
            # redshift.execute_statement(
            #     ClusterIdentifier='redshift-cluster-1',
            #     Database='dev',
            #     DbUser='awsuser',
            #     Sql=redshift_query,
            #     StatementName='InsertData',
            #     SecretArn=secret_arn
            # )

            # Append the data to the stream CSV file in S3
            # stream_data = pd.DataFrame({
            # "patient_id": [row["Patient ID"]],
            # "heart_rate": [row["Heart Rate"]],
            # "systolic_bp": [row["Systolic BP"]],
            # "diastolic_bp": [row["Diastolic BP"]],
            # "temperature": [row["Temperature"]],
            # "respiration_rate": [row["Respiration Rate"]],
            # "spo2": [row["SpO2"]],
            # "date_time": [row["Datetime"]]
            # })
        
        batch_df = batch_df.select(['Patient ID', 'Heart Rate', 'Systolic BP', 'Diastolic BP', 'Temperature', 'Respiration Rate','SpO2','Datetime'])
        #batch_df.show(1, truncate=False)
        batch_df.write.csv("s3://patient-vital-monitoring-system/patient_vitals/", header=True, mode="append")
        # stream_data = batch_df.toPandas()
        # s3_object = s3.get_object(Bucket=s3_bucket, Key=stream_csv_path)
        # existing_data = pd.read_csv(s3_object['Body'])
        # updated_data = pd.concat([existing_data, stream_data], ignore_index=True)
        # s3.put_object(Body=updated_data.to_csv(index=False), Bucket=s3_bucket, Key=stream_csv_path)

    # Check for threshold crossing and print alerts
    def process_alerts(batch_df):
        patient_name=''
        address=''
        #batch_df.write.csv("s3://patient-vital-monitoring-system/alerts/", header=True, mode="append")
        for row in batch_df.toLocalIterator():
            get_info = True          
            for column in threshold_values.keys():
                alert_column = f"{column}_alert"
                if row[alert_column] == 1:
                    # alert_query = f"""
                    # INSERT INTO public.alerts (patient_id, alert_metric, value, threshold_range)
                    # VALUES
                    # ({row["Patient ID"]}, '{column}', {row[column]}, '({threshold_values[column][0]}-{threshold_values[column][1]})') """
                    # #redshift.execute(alert_query)
                    # redshift.execute_statement(
                    #     ClusterIdentifier='redshift-cluster-1',
                    #     Database='dev',
                    #     DbUser='awsuser',
                    #     Sql=alert_query,
                    #     StatementName='InsertQueryData',
                    #     SecretArn=secret_arn
                    # )

                    alerts_data = pd.DataFrame({
                        "patient_id": [row["Patient ID"]],
                        "alert_metric": [column],
                        "value": [row[column]],
                        "threshold_range": [f"({threshold_values[column][0]}-{threshold_values[column][1]})"],
                        "alert_timestamp": [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
                    })
                    s3_object = s3.get_object(Bucket=s3_bucket, Key=alerts_csv_path)
                    existing_data = pd.read_csv(s3_object['Body'])
                    updated_data = pd.concat([existing_data, alerts_data], ignore_index=True)
                    s3.put_object(Body=updated_data.to_csv(index=False), Bucket=s3_bucket, Key=alerts_csv_path)
                    
                    #sns
                    #print(f"Alert: {column} crossed the threshold. Current value: {row[column]}, Threshold: {threshold_values[column]}")
                    if get_info:
                        # Execute the SELECT query
                        # select_query = f"SELECT patient_name, address FROM public.patients WHERE patient_id = {row['Patient ID']};"
                        # response = redshift.execute_statement(
                        #     ClusterIdentifier='redshift-cluster-1',
                        #     Database='dev',
                        #     DbUser='awsuser',
                        #     Sql=select_query,
                        #     StatementName='SelectData',
                        #     SecretArn=secret_arn
                        # )

                        # result = redshift.get_statement_result(
                        #         Id=response['Id'])
                        
                        # for row in result['Records']:
                        #     patient_name = row[0]['stringValue']
                        #     address = row[1]['stringValue']
                        s3_object = s3.get_object(Bucket=s3_bucket, Key=patient_csv_path)
                        patient_data = pd.read_csv(s3_object['Body'], usecols=['patient_id','patient_name','phone_number','age','admitted_ward','address'])

                        # Filter the patient data for the given patient_id
                        patient_info = patient_data[patient_data['patient_id'] == row['Patient ID']]

                        if not patient_info.empty:
                            # Extract patient name and address
                            patient_name = patient_info.iloc[0]['patient_name']
                            address = patient_info.iloc[0]['address']
                        get_info=False

                    alert = f"Alert:\nPatient: {patient_name},\nAddress: {address},\n{column} is abnormal.\nCurrent value: {row[column]:.2f}\nMust be in: {threshold_values[column]}"
                    response = sns.publish(
                                TopicArn=snsTopicArn,
                                Message=str(alert))
    
    for column, (min_threshold, max_threshold) in threshold_values.items():
        json_df = json_df.withColumn(f"{column}_alert", expr(f"IF(`{column}` < {min_threshold} OR `{column}` > {max_threshold}, 1, 0)"))
    
    # Filter the DataFrame to include only rows where any alert column is 1
    filtered_df = json_df.filter(expr(" OR ".join([f"`{column}_alert` = 1" for column in threshold_values.keys()])))

    # Define the streaming query and output the results
    # query_load = json_df \
    #     .writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(lambda batch_df, batch_id: load_stream(batch_df)) \
    #     .start()

    query_load = json_df \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(lambda batch_df, batch_id: load_stream(batch_df)) \
        .start()
    query_alert = filtered_df \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(lambda batch_df, batch_id: process_alerts(batch_df)) \
        .start()

    # query_alert = filtered_df \
    #     .writeStream \
    #     .outputMode("append") \
    #     .foreachBatch(process_alerts) \
    #     .start()

    query_load.awaitTermination()
    query_alert.awaitTermination()

#---------------------------Using Kafka-python-------------------------------------

# consumer = KafkaConsumer(
#     'patient_vitals',
#     bootstrap_servers='localhost:9092',
#     auto_offset_reset='earliest',
#     group_id='my-group',
#     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
# )

# if __name__ == "__main__":
#     for message in consumer:
#         vitals = message.value
#         print("Received Vitals:", vitals)
#         check_thresholds(vitals)