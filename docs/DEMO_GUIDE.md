# Demo Guide

A ~10 minute walkthrough. Start the stack **3 minutes before** you present so the dashboard is already filled.

## Before the demo

1. Start Docker Desktop and wait until it says "running".
2. From the project root, `docker compose up -d --build`
3. Check all containers are up: `docker compose ps`
4. Open http://localhost:8501 and confirm the numbers are increasing.
5. Open http://localhost:9870 in a second tab (Utilities → Browse the file system → `/patient_vitals`).

## Script

**1. The problem (1 min).** Hospitals generate continuous vitals; delayed detection of deterioration costs lives. Our pipeline detects abnormal readings within seconds. (Volume, velocity, variety, veracity: see CASE_STUDY_MAPPING.md.)

**2. The architecture (2 min).** Show the diagram in README.md. Walk through: producer → Kafka → Spark → HDFS + alerts topic → dashboard.

**3. Live dashboard (3 min).**
- Top tiles: readings and alerts counters climbing.
- "Live vitals": pick a patient and a metric (try `spo2` and `heart_rate`), the line moves every 3 s.
- "Latest reading per patient": all 10 monitored patients at a glance.
- "Recent alerts": each row shows patient name, ward, which metric, LOW/HIGH, value vs. normal range, and WARNING/CRITICAL.

**4. Show the processing (1 min).** Show the Spark log lines, one per micro-batch:
```bash
docker logs -f pv-spark
```
Output looks like `batch 12: 100 readings, 5 alerts`. Press Ctrl+C to leave.

**5. Show the storage (2 min).**
- Dashboard bottom section "Stored in HDFS": size and file counts grow.
- In the NameNode UI (http://localhost:9870), open `/patient_vitals/vitals` (parquet, partitioned by date) and `/patient_vitals/alerts` (csv).
- Optional: read it back with Spark SQL, see below.

**6. HBase: instant patient lookup (1 min).** Scroll to "Patient lookup (HBase)" at the bottom of the dashboard, choose a patient id and click **Look up in HBase**. It shows that patient's latest vitals and most recent alerts and how long it took (tens of milliseconds). Explain: HBase is for "pull up this one patient now"; HDFS/Hive are for analysing everything. The HBase master UI is at http://localhost:16010 (Table Details shows `latest_vitals` and `alerts`).

**7. Hive: SQL analytics (2 min).** Run the Hive queries below in a terminal: average heart rate per patient, and alerts by metric and severity. Explain that Hive does not copy data: the tables are definitions over the files Spark wrote to HDFS. Queries take several seconds because Hive 2.3 runs them as MapReduce jobs.

**8. Wrap-up (1 min).** Mention limits and future scope: readmission prediction ML (Spark MLlib), Cassandra for time-series, Sqoop for importing EHR databases, Solr for clinical-note search.

## Useful live commands

Watch alerts arrive on the Kafka topic:
```bash
docker exec pv-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic patient-alerts --from-beginning --max-messages 5
```

Hive: alerts by metric and severity (in PowerShell, keep the SQL in a variable):
```powershell
$q = 'SELECT metric, severity, count(*) AS n FROM patient_vitals.alerts GROUP BY metric, severity ORDER BY n DESC'
docker exec pv-hive-server beeline -u jdbc:hive2://localhost:10000 -e $q
```
Hive: average heart rate per patient, and alerts per ward:
```powershell
$q = 'SELECT patient_id, count(*) AS readings, round(avg(heart_rate),1) AS avg_hr FROM patient_vitals.vitals GROUP BY patient_id ORDER BY patient_id'
docker exec pv-hive-server beeline -u jdbc:hive2://localhost:10000 -e $q
$q = 'SELECT admitted_ward, count(*) AS alerts, sum(CASE WHEN severity=''CRITICAL'' THEN 1 ELSE 0 END) AS critical FROM patient_vitals.alerts GROUP BY admitted_ward ORDER BY alerts DESC'
docker exec pv-hive-server beeline -u jdbc:hive2://localhost:10000 -e $q
```
Open Hive's web UI at http://localhost:10002 to show running queries.

HBase: read tables with the HBase shell:
```bash
docker exec -it pv-hbase hbase shell
```
then at the `hbase>` prompt:
```
scan 'latest_vitals', {LIMIT => 2}
get 'latest_vitals', '00007'
scan 'alerts', {STARTROW => '00007#', STOPROW => '00007$', LIMIT => 3}
```
(`00007$` works as the stop row because `$` sorts just after `#`.)

List files in HDFS:
```bash
docker exec pv-namenode hdfs dfs -ls -R /patient_vitals
```

Count the stored readings with Spark SQL (a one-off job, run while the stream is also running):
```bash
docker exec pv-spark /opt/spark/bin/spark-sql --master local[1] --conf spark.jars.ivy=/tmp/ivy -e 'SELECT patient_id, count(*) AS readings, round(avg(heart_rate),1) AS avg_hr FROM parquet.`hdfs://namenode:9000/patient_vitals/vitals` GROUP BY patient_id ORDER BY patient_id'
```

## Make alerts more (or less) frequent for the demo

See CONFIGURATION.md: change thresholds in `spark/stream_job.py`, then `docker compose up -d --build spark-streaming` (restart the job only).

## After the demo

`docker compose down` keeps data; `docker compose down -v` deletes all stored data.
