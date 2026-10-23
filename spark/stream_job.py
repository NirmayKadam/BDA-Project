"""Spark Structured Streaming job: Kafka -> threshold alerting -> HDFS (+ Kafka alerts topic).

Replaces the original repo's AWS pieces:
  AWS SNS      -> Kafka topic `patient-alerts` (consumed by the dashboard)
  AWS S3       -> HDFS (parquet for vitals, CSV for alerts)
  Redshift     -> not needed (dashboard reads HDFS / Kafka directly)
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                               StructType)

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
HDFS = os.environ.get("HDFS_BASE", "hdfs://namenode:9000/patient_vitals")
IN_TOPIC = "patient-vitals"
OUT_TOPIC = "patient-alerts"

# metric -> (low, high). Temperature is in Fahrenheit (that is what the dataset uses).
THRESHOLDS = {
    "heart_rate": (50, 110),
    "systolic_bp": (80, 150),
    "diastolic_bp": (50, 95),
    "temperature": (95.0, 100.4),
    "respiration_rate": (8, 24),
    "spo2": (93.5, 100.0),
}
# Beyond this fractional distance outside the normal range an alert is CRITICAL, else WARNING.
CRITICAL_FRACTION = 0.05

spark = (SparkSession.builder.appName("PatientVitalsAlerting").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("patient_id", IntegerType()),
    StructField("heart_rate", DoubleType()),
    StructField("systolic_bp", DoubleType()),
    StructField("diastolic_bp", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("respiration_rate", DoubleType()),
    StructField("spo2", DoubleType()),
    StructField("sim_time", StringType()),
    StructField("event_time", StringType()),
])

# Static patient master data (500k rows, only a handful are ever matched) -> broadcast join.
patients = (spark.read.option("header", True).option("inferSchema", True)
            .csv("/data/patients.csv")
            .select("patient_id", "patient_name", "age", "admitted_ward", "phone_number"))

vitals = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", KAFKA)
          .option("subscribe", IN_TOPIC)
          .option("startingOffsets", "latest")
          .option("failOnDataLoss", "false")
          .load()
          .select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
          .select("d.*")
          .withColumn("event_time", F.to_timestamp("event_time")))


def build_alerts(df):
    """One output row per violated metric."""
    checks = []
    for metric, (lo, hi) in THRESHOLDS.items():
        v = F.col(metric)
        below = v < lo
        above = v > hi
        frac = F.when(below, (F.lit(lo) - v) / F.lit(lo)).otherwise((v - F.lit(hi)) / F.lit(hi))
        checks.append(F.when(below | above, F.struct(
            F.lit(metric).alias("metric"),
            v.alias("value"),
            F.lit(lo).alias("normal_low"),
            F.lit(hi).alias("normal_high"),
            F.when(below, F.lit("LOW")).otherwise(F.lit("HIGH")).alias("direction"),
            F.when(frac > CRITICAL_FRACTION, F.lit("CRITICAL")).otherwise(F.lit("WARNING")).alias("severity"),
        )))
    exploded = (df.withColumn("a", F.explode(F.filter(F.array(*checks), lambda x: x.isNotNull())))
                .select("patient_id", "event_time", "sim_time", "a.*"))
    return exploded.join(F.broadcast(patients), "patient_id", "left")


def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    batch_df.persist()
    n = batch_df.count()

    # 1) raw vitals -> HDFS parquet (partitioned by day)
    (batch_df.withColumn("date", F.to_date("event_time"))
     .write.mode("append").partitionBy("date").parquet(f"{HDFS}/vitals"))

    # 2) alerts -> HDFS csv + Kafka topic
    alerts = build_alerts(batch_df).persist()
    n_alerts = alerts.count()
    if n_alerts:
        alerts.write.mode("append").option("header", True).csv(f"{HDFS}/alerts")
        (alerts.select(F.col("patient_id").cast("string").alias("key"),
                       F.to_json(F.struct(*alerts.columns)).alias("value"))
         .write.format("kafka")
         .option("kafka.bootstrap.servers", KAFKA)
         .option("topic", OUT_TOPIC)
         .save())
    print(f"batch {batch_id}: {n} readings, {n_alerts} alerts", flush=True)
    alerts.unpersist()
    batch_df.unpersist()


query = (vitals.writeStream
         .foreachBatch(process_batch)
         .option("checkpointLocation", f"{HDFS}/_checkpoints/alerting")
         .trigger(processingTime="5 seconds")
         .start())
query.awaitTermination()
