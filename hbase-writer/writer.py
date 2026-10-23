"""Kafka -> HBase serving layer.

HBase is used for what it is good at: instant random reads by key.
  table `latest_vitals`: row key = patient id          -> the newest reading per patient (overwritten in place)
  table `alerts`      : row key = patient id # reverse timestamp -> a patient's alerts, newest first

Reading a patient's current state or recent alerts is a single key/prefix lookup, no scan of the full history.
"""
import json
import os
import threading
import time
from datetime import datetime

import happybase
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
HBASE_HOST = os.environ.get("HBASE_HOST", "hbase")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))


def connect_hbase():
    for attempt in range(1, 61):
        try:
            conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=10000)
            conn.tables()
            return conn
        except Exception as e:  # thrift not up yet
            print(f"HBase not ready (attempt {attempt}): {e}", flush=True)
            time.sleep(5)
    raise SystemExit("Could not reach HBase")


def ensure_tables(conn):
    existing = {t.decode() for t in conn.tables()}
    if "latest_vitals" not in existing:
        conn.create_table("latest_vitals", {"v": dict(max_versions=1)})
    if "alerts" not in existing:
        conn.create_table("alerts", {"a": dict(max_versions=1)})


def consumer(topic):
    while True:
        try:
            return KafkaConsumer(topic, bootstrap_servers=KAFKA, auto_offset_reset="latest",
                                 group_id=f"hbase-writer-{topic}",
                                 value_deserializer=lambda m: json.loads(m.decode()))
        except NoBrokersAvailable:
            time.sleep(3)


def to_bytes(d):
    return {k.encode(): str(v).encode() for k, v in d.items()}


def write_vitals(conn):
    table = conn.table("latest_vitals")
    n = 0
    for msg in consumer("patient-vitals"):
        r = msg.value
        row = f"{int(r['patient_id']):05d}".encode()
        table.put(row, to_bytes({f"v:{k}": v for k, v in r.items() if k != "patient_id"}))
        n += 1
        if n % 500 == 0:
            print(f"latest_vitals: {n} puts", flush=True)


def write_alerts(conn):
    table = conn.table("alerts")
    n = 0
    for msg in consumer("patient-alerts"):
        r = msg.value
        # reverse timestamp so a prefix scan returns the newest alert first
        ts = datetime.now().timestamp()
        rev = 9_999_999_999_999 - int(ts * 1000)
        row = f"{int(r['patient_id']):05d}#{rev}#{r['metric']}".encode()
        table.put(row, to_bytes({f"a:{k}": v for k, v in r.items() if k != "patient_id"}))
        n += 1
        if n % 50 == 0:
            print(f"alerts: {n} puts", flush=True)


def main():
    conn = connect_hbase()
    ensure_tables(conn)
    print("HBase tables ready", flush=True)
    # separate connections: happybase connections are not thread-safe
    threading.Thread(target=write_alerts, args=(connect_hbase(),), daemon=True).start()
    write_vitals(conn)


if __name__ == "__main__":
    main()
