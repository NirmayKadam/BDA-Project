"""Simulated bedside monitors: replays the 3-night synthetic vitals file into Kafka.

Each CSV row is one 30-second reading for one patient. We send rows at MSGS_PER_SEC
(instead of real time) so a demo shows data immediately, and stamp each message with the
current wall-clock time as `event_time`.
"""
import csv
import json
import os
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("VITALS_TOPIC", "patient-vitals")
CSV_PATH = os.environ.get("CSV_PATH", "/data/vitals.csv")
RATE = float(os.environ.get("MSGS_PER_SEC", "20"))
LOOP = os.environ.get("LOOP", "true").lower() == "true"


def connect():
    for attempt in range(1, 31):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready (attempt {attempt}), retrying...", flush=True)
            time.sleep(3)
    raise SystemExit("Could not reach Kafka")


def rows():
    with open(CSV_PATH, newline="") as f:
        for r in csv.DictReader(f):
            yield {
                "patient_id": int(r["Patient ID"]),
                "heart_rate": float(r["Heart Rate"]),
                "systolic_bp": float(r["Systolic BP"]),
                "diastolic_bp": float(r["Diastolic BP"]),
                "temperature": float(r["Temperature"]),        # degrees Fahrenheit
                "respiration_rate": float(r["Respiration Rate"]),
                "spo2": float(r["SpO2"]),
                "sim_time": r["Datetime"],                      # time inside the simulated dataset
            }


def main():
    producer = connect()
    delay = 1.0 / RATE
    sent = 0
    print(f"Producing to {TOPIC} at {RATE}/s from {CSV_PATH}", flush=True)
    while True:
        for msg in rows():
            msg["event_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            producer.send(TOPIC, key=str(msg["patient_id"]), value=msg)
            sent += 1
            if sent % 500 == 0:
                producer.flush()
                print(f"sent {sent} messages", flush=True)
            time.sleep(delay)
        producer.flush()
        if not LOOP:
            break
    print("done", flush=True)


if __name__ == "__main__":
    main()
