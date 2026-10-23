# Real-Time Patient Vital Monitoring: Local Big Data Demo

A fully local (no AWS, no API keys) version of the
[Real-Time Patient Vital Monitoring System](https://github.com/SwaroopMeher/REAL-TIME-PATIENT-VITAL-MONITORING-SYSTEM).
Synthetic ICU vitals stream through **Kafka**, are analysed by **Spark Structured Streaming**,
stored in **HDFS**, queried with SQL in **Hive**, served for instant per-patient lookups from **HBase**,
and shown on a live **Streamlit dashboard**.

```
 producer ──► Kafka ──► Spark Structured Streaming ──► HDFS (vitals parquet, alerts csv) ──► Hive (SQL)
 (synthetic   topic:        • threshold checks            ▲
  bedside     patient-      • patient enrichment          │ WebHDFS
  monitors)   vitals        • writes alerts back  ──► Kafka topic: patient-alerts
                 │                                            │
                 └───────────────┬────────────────────────────┘
                                 ▼
                      hbase-writer ──► HBase (latest vitals + alerts per patient)
                                 │
                                 ▼
                      Streamlit dashboard :8501  (live Kafka view, HDFS stats, HBase lookup)
```

## Quick start

Requirements: Docker Desktop (give it at least 8 GB RAM; the full stack of 12 containers uses about 5 GB) and an internet connection for the first run
(images and the Spark-Kafka connector jar are downloaded once).

```bash
cd "D:\Nirmay pc\ENGINEERING\projects\BDA_Project"
docker compose up -d --build
```

Then open **http://localhost:8501**. The first start takes 1-3 minutes while Spark downloads its
Kafka connector; the dashboard shows "Waiting for data" until the first batch arrives.

Stop everything (data kept): `docker compose down`
Stop and wipe all data (HDFS + checkpoints): `docker compose down -v`

## Addresses

| What | URL |
|---|---|
| Dashboard | http://localhost:8501 |
| HDFS NameNode UI (browse files at Utilities → Browse the file system) | http://localhost:9870 |
| Spark UI (only while the job runs) | http://localhost:4040 |
| HBase master UI | http://localhost:16010 |
| HiveServer2 UI | http://localhost:10002 |
| HiveServer2 JDBC | `jdbc:hive2://localhost:10000` |
| Kafka from the host (optional) | `localhost:29092` |

## What is in this folder

| Path | Purpose |
|---|---|
| `docker-compose.yml` | Defines all 6 services |
| `producer/producer.py` | Replays the 3-night synthetic vitals file into Kafka |
| `spark/stream_job.py` | The streaming job: threshold alerting, enrichment, HDFS and Kafka output |
| `dashboard/app.py` | Streamlit dashboard (live view, HDFS stats, HBase patient lookup) |
| `hbase-writer/writer.py` | Copies the latest vitals and alerts from Kafka into HBase tables |
| `hive/init.sql` | Creates the Hive database and the external tables over the HDFS files |
| `docs/` | Architecture, demo script, case-study mapping, troubleshooting, configuration |
| `data/` | Synthetic input data |
| `reference/original-aws-repo/` | The upstream AWS-based project this was derived from, kept for reference only (not used when running) |

## Data

All data is synthetic and already in the repo (nothing to download):
- `data/patient_vitals_3nights.csv`: 43,200 readings, 10 patients, one reading each every 30 s for 3 nights
- `data/patients.csv`: 500,000 synthetic patient records (name, age, ward, phone, address)

Read the docs in `docs/` next:
1. [ARCHITECTURE.md](docs/ARCHITECTURE.md): how it works
2. [DEMO_GUIDE.md](docs/DEMO_GUIDE.md): step-by-step script for presenting it
3. [CASE_STUDY_MAPPING.md](docs/CASE_STUDY_MAPPING.md): maps this project to the healthcare case study
4. [CONFIGURATION.md](docs/CONFIGURATION.md): thresholds, speed, ports
5. [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)
