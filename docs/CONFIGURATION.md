# Configuration

## Alert thresholds

`spark/stream_job.py`, dictionary `THRESHOLDS` (metric → low, high):

| Metric | Low | High | Unit |
|---|---|---|---|
| heart_rate | 50 | 110 | bpm |
| systolic_bp | 80 | 150 | mmHg |
| diastolic_bp | 50 | 95 | mmHg |
| temperature | 95.0 | 100.4 | °F |
| respiration_rate | 8 | 24 | breaths/min |
| spo2 | 93.5 | 100 | % |

`CRITICAL_FRACTION = 0.05`: an alert is CRITICAL when the value is more than 5% beyond the bound, otherwise WARNING.

These are tuned to the synthetic data (normal readings are centred on HR 72, systolic 115, diastolic 75, temperature 97 °F, respiration 17, SpO2 97), so only a few percent of readings alert. They are demo values, not clinical guidance.

After editing, restart only the Spark service:
```bash
docker compose up -d --build spark-streaming
```
(The service mounts the file, so `docker compose restart spark-streaming` also works.)

## Stream speed

`docker-compose.yml`, service `producer`:
- `MSGS_PER_SEC`: default 20. Raise to 100+ to show throughput; lower to 2 to show individual readings.
- `LOOP`: `true` replays the file forever; `false` sends it once.

## Ports

If a port is already used on your machine, change the left side of the mapping in `docker-compose.yml`: dashboard `8501`, HDFS UI `9870`, Spark UI `4040`, Kafka host port `29092`, HBase UI `16010`, HiveServer2 `10000` and its UI `10002`.

## Memory

Docker Desktop → Settings → Resources: 8 GB recommended. With Hive and HBase the stack is 12 containers and uses roughly 5 GB.
To save memory during a quick Kafka/Spark/HDFS-only demo you can stop the extras: `docker compose stop hive-server hive-metastore hive-metastore-db hbase hbase-writer`. The Spark driver is limited to 1 GB (`spark.driver.memory`).

## Changing the dataset

Replace `data/patient_vitals_3nights.csv` with any CSV with the same columns:
`Patient ID,Heart Rate,Systolic BP,Diastolic BP,Temperature,Respiration Rate,SpO2,Datetime`.
Patient ids should exist in `data/patients.csv` to get names and wards on alerts.
