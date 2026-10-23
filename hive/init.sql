-- Hive tables over the files Spark writes to HDFS (external = Hive never deletes the data).
CREATE DATABASE IF NOT EXISTS patient_vitals;
USE patient_vitals;

-- Vitals: Parquet, partitioned by date (Spark writes /patient_vitals/vitals/date=YYYY-MM-DD/)
CREATE EXTERNAL TABLE IF NOT EXISTS vitals (
  patient_id       INT,
  heart_rate       DOUBLE,
  systolic_bp      DOUBLE,
  diastolic_bp     DOUBLE,
  temperature      DOUBLE,
  respiration_rate DOUBLE,
  spo2             DOUBLE,
  sim_time         STRING,
  event_time       TIMESTAMP
)
PARTITIONED BY (`date` DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/patient_vitals/vitals';

-- Alerts: CSV with a header line in every part file
CREATE EXTERNAL TABLE IF NOT EXISTS alerts (
  patient_id    INT,
  event_time    STRING,   -- Spark writes ISO-8601 text in CSV, which Hive TIMESTAMP cannot parse
  sim_time      STRING,
  metric        STRING,
  value         DOUBLE,
  normal_low    DOUBLE,
  normal_high   DOUBLE,
  direction     STRING,
  severity      STRING,
  patient_name  STRING,
  age           INT,
  admitted_ward INT,
  phone_number  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/patient_vitals/alerts'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Register the date partitions that already exist in HDFS
MSCK REPAIR TABLE vitals;
