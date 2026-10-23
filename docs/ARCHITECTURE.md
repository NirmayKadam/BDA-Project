# Architecture

## Components

| Service (container) | Image | Role |
|---|---|---|
| `producer` (pv-producer) | built from `producer/` (python 3.11) | Simulates bedside monitors. Reads the CSV and publishes one JSON message per patient reading to Kafka topic `patient-vitals`. |
| `kafka` (pv-kafka) | apache/kafka 3.8.0, KRaft mode | Message broker, no ZooKeeper needed. Buffers the stream. Topics: `patient-vitals` (input), `patient-alerts` (output). |
| `spark-streaming` (pv-spark) | apache/spark 3.5.3 | Runs `stream_job.py` in local mode (`local[2]`). |
| `namenode` / `datanode` | bde2020/hadoop 3.2.1 | A single-node HDFS: the persistent storage layer. |
| `hbase` (pv-hbase) | harisekhon/hbase 2.1 (standalone) | NoSQL store for fast lookups by patient. Exposes Thrift on port 9090 inside the Docker network. |
| `hbase-writer` (pv-hbase-writer) | built from `hbase-writer/` | Reads both Kafka topics and writes to HBase tables `latest_vitals` and `alerts`. |
| `hive-metastore-db` / `hive-metastore` / `hive-server` | bde2020/hive 2.3.2 + PostgreSQL | Hive: stores table definitions (metastore) and answers SQL (HiveServer2) over the files Spark wrote to HDFS. |
| `hive-init` (pv-hive-init) | same Hive image | One-shot job that runs `hive/init.sql` (database, tables, partition registration), then exits. |
| `dashboard` (pv-dashboard) | built from `dashboard/` (Streamlit) | Live view from Kafka, stored-data view from HDFS (via WebHDFS) and patient lookup from HBase. |

## Data flow

1. **Ingest.** The producer sends rows at `MSGS_PER_SEC` (default 20). Every message carries `patient_id`, the six vitals, `sim_time` (timestamp inside the dataset) and `event_time` (wall clock when sent). The Kafka message key is the patient id, so a patient's readings stay ordered.
2. **Process.** Spark reads `patient-vitals` with Structured Streaming and runs a micro-batch every 5 seconds. For each batch (`process_batch` in `stream_job.py`):
   - writes all readings to HDFS as Parquet at `/patient_vitals/vitals`, partitioned by date;
   - compares each reading with the thresholds and creates one alert row per violated metric (a single reading may violate several);
   - classifies each alert **WARNING** or **CRITICAL** (more than 5% outside the normal range = critical);
   - enriches alerts with patient name, age, ward and phone through a broadcast join with `patients.csv`;
   - writes alerts to HDFS as CSV at `/patient_vitals/alerts` and publishes them as JSON to Kafka topic `patient-alerts`.
3. **Serve.** The dashboard keeps two Kafka consumers running in background threads (live vitals and live alerts). Separately it calls WebHDFS to report stored size and file counts and to read back alert history.

4. **Serving layer (HBase).** `hbase-writer` consumes both topics independently of Spark. For each reading it overwrites one row in `latest_vitals` (row key = zero-padded patient id, e.g. `00007`). For each alert it adds a row in `alerts` with key `patientid#reverseTimestamp#metric`; the reversed timestamp makes a prefix scan return a patient's newest alerts first. The dashboard's "Patient lookup" button reads these tables, so one patient's current state or recent alerts come back in milliseconds instead of scanning the full history.
5. **Analytics layer (Hive).** Hive does not copy data. `hive/init.sql` creates *external* tables that point at the HDFS folders Spark writes: `patient_vitals.vitals` (Parquet, partitioned by date) and `patient_vitals.alerts` (CSV). Analysts then use plain SQL (`GROUP BY`, `JOIN`, averages) with `beeline`. Hive runs its queries as MapReduce jobs (local mode here, no YARN), so this is also where MapReduce appears. When a new date's data first appears, run `hive/refresh.ps1` (`MSCK REPAIR TABLE`) so Hive discovers the new partition.

Spark checkpoints live in HDFS (`/patient_vitals/_checkpoints/alerting`), so restarting the job continues where it stopped.

## How this differs from the original repo

| Original | This version | Why |
|---|---|---|
| AWS SNS email/SMS alerts | Kafka topic `patient-alerts` + dashboard | No cloud account needed |
| AWS S3 storage | HDFS | Uses the Hadoop ecosystem, runs locally |
| Redshift + Metabase | Streamlit dashboard reading Kafka / HDFS | One less service, simple |
| Kafka + ZooKeeper (bitnami images) | Kafka KRaft (official image) | Fewer containers; bitnami images are no longer reliably available |
| Scripts using localhost and `trigger(once=True)` | One continuous Spark job | Real streaming behaviour |
| Thresholds in `threshold_values.csv`, temperature in °C while the data is °F | Thresholds in `stream_job.py`, °F | The data file is in Fahrenheit, so the original °C thresholds would flag everything |

The original project is kept unmodified in `reference/original-aws-repo/` (credit: SwaroopMeher, github.com/SwaroopMeher/REAL-TIME-PATIENT-VITAL-MONITORING-SYSTEM). Only its two data files were copied into `data/`.

## Why these technology choices (useful for viva questions)

- **Kafka** decouples producers from consumers, buffers bursts and lets several consumers (Spark, dashboard) read the same stream independently.
- **Spark Structured Streaming** gives the same DataFrame API for batch and stream, scales out with more cores/nodes, and supports exactly-once file sinks through checkpoints.
- **HDFS** provides cheap, replicated, append-only storage for the growing history; Parquet makes later analytical queries (Spark SQL, Hive) fast.
- **HBase** is a key-value store built for millisecond random reads and writes. HDFS/Hive are the opposite: great for scanning everything, slow for "show me patient 7 right now". So each has its own job.
- **Hive** gives SQL access to files in HDFS without writing Spark code, which suits analysts and compliance-style reports.
- **Micro-batch (5 s)** trades a little latency for throughput and fault tolerance.

## Limits (be honest in the presentation)

- Single-node everything: HDFS replication is 1, Spark is `local[2]`, Kafka has one partition.
- HBase runs in standalone mode and keeps its data inside its own container, not on HDFS, and the tables are rebuilt from the live stream (only readings that arrive after the writer starts). In production HBase sits on HDFS with ZooKeeper.
- Hive 2.3 is an old release (it matches the Hadoop 2.x images available); queries run as local MapReduce, so they take several seconds even for small data. The code is the same as on a cluster; only the configuration would change.
- Alert rules are fixed thresholds, not machine learning.
- No authentication or encryption. Data is synthetic; a real deployment handling patient data needs HIPAA-grade security.
