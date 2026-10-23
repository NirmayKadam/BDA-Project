# Mapping to the Healthcare Big Data Case Study

## The four V's

| V | In the case study | In this project |
|---|---|---|
| **Volume** | Petabytes of EHR, imaging, genomics | 43,200 vitals readings replayed continuously + 500,000 patient records; HDFS storage that grows as the stream runs. Scales by adding nodes. |
| **Velocity** | ICU monitor/IoT streams needing real-time alerts | Streaming ingestion through Kafka, 5-second Spark micro-batches, alerts visible within seconds. |
| **Variety** | Structured, semi-structured, unstructured | JSON streams (Kafka), CSV master data (patients), Parquet and CSV in HDFS. (Unstructured clinical notes/imaging are out of scope.) |
| **Veracity** | Accurate, clean data | Typed schema enforced when parsing JSON; units made consistent (°F); invalid/missing fields become nulls rather than false alerts. |

## Application areas

| Case-study application | Covered? | Where |
|---|---|---|
| Real-time alerting and wearables | **Yes (core)** | Producer simulates IoT monitors; Spark job raises WARNING/CRITICAL alerts |
| Patient risk stratification | Partly | Severity levels per alert; per-ward / per-patient alert counts on the dashboard |
| Electronic health records | Partly | Patient master data joined with the stream and stored in HDFS; instant patient lookup from HBase; SQL reports through Hive |
| Predictive analytics (readmission) | Future scope | Stored Parquet history is ready for Spark MLlib |
| Medical imaging | No | Out of scope |

## Hadoop ecosystem components

| Component | Used? | Notes |
|---|---|---|
| HDFS | **Yes** | Stores vitals (Parquet) and alerts (CSV), and Spark checkpoints |
| YARN | No | Spark runs in local mode. On a cluster you would submit with `--master yarn`. |
| MapReduce | **Indirectly** | Hive 2.3 executes its SQL queries as MapReduce jobs (local mode). Streaming itself uses Spark, the faster engine the case study also lists. |
| Apache Spark / Spark Streaming | **Yes** | Structured Streaming job |
| Apache Kafka | **Yes** | Ingestion of vitals and delivery of alerts |
| Hive | **Yes** | External tables over the vitals (Parquet) and alerts (CSV) in HDFS; SQL analytics through HiveServer2/beeline |
| HBase | **Yes** | Latest vitals and alert history per patient, for instant lookup (the case study's "pull up a patient's history in an emergency"). Runs standalone. |
| Cassandra | No | The case study uses it for high-velocity time-series; HBase covers the NoSQL role here |
| Sqoop | No | Would import EHR tables from an RDBMS; here patients come from CSV |
| ZooKeeper | Embedded only | Kafka runs in KRaft mode, so it needs none. The standalone HBase image bundles its own internal ZooKeeper. |
| Mahout | No | Spark MLlib is the modern equivalent |
| Solr / Lucene | No | For clinical-note search; out of scope |

## Suggested architecture slide

- Data sources: simulated bedside monitors (CSV replay), patient master data (CSV)
- Data integration: Kafka
- Processing: Spark Structured Streaming
- Storage: HDFS (history), HBase (fast per-patient lookups), Hive tables (SQL on HDFS)
- Application layer: real-time vitals alerting, dashboard, patient lookup, SQL analytics
- Future: readmission prediction (MLlib), Cassandra, Sqoop, Solr
