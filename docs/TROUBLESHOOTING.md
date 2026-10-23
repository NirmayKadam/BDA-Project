# Troubleshooting

## Dashboard shows "Waiting for data"
Normal for the first 1-3 minutes. Spark downloads its Kafka connector on first start. Check:
```bash
docker logs pv-spark --tail 30
```
You are looking for lines such as `batch 3: 100 readings, 4 alerts`.

## A container keeps restarting
```bash
docker compose ps
docker logs <container-name> --tail 50
```
Container names: `pv-kafka`, `pv-namenode`, `pv-datanode`, `pv-spark`, `pv-producer`, `pv-dashboard`.

## Spark cannot reach HDFS / "could only be written to 0 of 1 datanodes"
The DataNode is not up yet or has no space. Wait 30 s; check `docker logs pv-datanode`. If the problem remains after you changed volumes, reset: `docker compose down -v` then `docker compose up -d --build`.

## NameNode / DataNode "incompatible clusterID" after a reset
The two volumes got out of sync. Run `docker compose down -v` (wipes both) and start again.

## Spark fails to download the Kafka package
It needs internet on the first run. Once downloaded it is cached in the `spark_ivy` volume.

## Port already in use
Edit the left side of the port mapping in `docker-compose.yml` (see CONFIGURATION.md).

## Out of memory / everything is slow
Raise Docker Desktop's memory to 6-8 GB, close other heavy apps, or lower `MSGS_PER_SEC`.

## Hive: tables missing or empty
- `pv-hive-init` runs once and exits. Check `docker logs pv-hive-init`; if it failed, re-run `docker compose up -d hive-init` after HiveServer2 is up (about 1 minute after start).
- Hive shows no rows for a new day: run `hive/refresh.ps1` to register the new partition.
- Queries take several seconds: normal, Hive 2.3 runs MapReduce jobs in local mode.
- Out of memory in `pv-hive-server`: give Docker more RAM or stop other containers.

## HBase: "not reachable yet" on the dashboard, or empty tables
- HBase takes 30-60 seconds to start. `docker logs pv-hbase-writer` should end with `HBase tables ready`.
- The tables only fill with readings that arrive after the writer starts, and they are lost when the `pv-hbase` container is recreated (`docker compose down`). They refill within seconds of the next start.

## Re-run from scratch
```bash
docker compose down -v
docker compose up -d --build
```

## No alerts appear
Thresholds may be too loose for the data; tighten them (see CONFIGURATION.md). With the defaults a few percent of readings should alert.

## Stop the producer only (keep the rest running)
```bash
docker compose stop producer
```
Start again with `docker compose start producer`.
