"""Simple live dashboard: Kafka (live vitals + alerts) and HDFS (stored data)."""
import io
import json
import os
import threading
import time
from collections import deque

import pandas as pd
import requests
import streamlit as st
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
WEBHDFS = os.environ.get("WEBHDFS", "http://namenode:9870/webhdfs/v1")
ALERTS_DIR = os.environ.get("HDFS_ALERTS_DIR", "/patient_vitals/alerts")

st.set_page_config(page_title="Patient Vitals Monitor", layout="wide")


@st.cache_resource
def start_consumers():
    """Background threads keep the latest messages of both topics in memory."""
    store = {"vitals": deque(maxlen=3000), "alerts": deque(maxlen=1000),
             "vitals_total": 0, "alerts_total": 0}

    def run(topic, key):
        consumer = None
        while consumer is None:
            try:
                consumer = KafkaConsumer(topic, bootstrap_servers=KAFKA,
                                         auto_offset_reset="earliest",
                                         value_deserializer=lambda m: json.loads(m.decode()))
            except NoBrokersAvailable:
                time.sleep(3)
        for msg in consumer:
            store[key].append(msg.value)
            store[key + "_total"] += 1

    for topic, key in (("patient-vitals", "vitals"), ("patient-alerts", "alerts")):
        threading.Thread(target=run, args=(topic, key), daemon=True).start()
    return store


def hdfs_summary(path):
    try:
        r = requests.get(f"{WEBHDFS}{path}", params={"op": "GETCONTENTSUMMARY"}, timeout=3)
        return r.json()["ContentSummary"]
    except Exception:
        return None


def hdfs_read_alerts(max_files=40):
    """Read the most recent alert CSV part files from HDFS through WebHDFS."""
    try:
        r = requests.get(f"{WEBHDFS}{ALERTS_DIR}", params={"op": "LISTSTATUS"}, timeout=3)
        files = [f for f in r.json()["FileStatuses"]["FileStatus"]
                 if f["pathSuffix"].endswith(".csv") and f["length"] > 0]
        files.sort(key=lambda f: f["modificationTime"], reverse=True)
        frames = []
        for f in files[:max_files]:
            data = requests.get(f"{WEBHDFS}{ALERTS_DIR}/{f['pathSuffix']}",
                                params={"op": "OPEN"}, timeout=5).content
            frames.append(pd.read_csv(io.BytesIO(data)))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


store = start_consumers()

st.title("Real-Time Patient Vital Monitoring")
st.caption("Kafka -> Spark Structured Streaming -> HDFS. Live view refreshes every 3 seconds.")


@st.fragment(run_every=3)
def live():
    vitals = pd.DataFrame(list(store["vitals"]))
    alerts = pd.DataFrame(list(store["alerts"]))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Readings received (this session)", f"{store['vitals_total']:,}")
    c2.metric("Alerts raised (this session)", f"{store['alerts_total']:,}")
    crit = int((alerts["severity"] == "CRITICAL").sum()) if not alerts.empty else 0
    c3.metric("Critical alerts (recent)", crit)
    c4.metric("Patients monitored", vitals["patient_id"].nunique() if not vitals.empty else 0)

    if vitals.empty:
        st.info("Waiting for data... (the producer and Spark job take ~1-2 minutes to warm up on first start)")
        return

    vitals["event_time"] = pd.to_datetime(vitals["event_time"])
    left, right = st.columns([3, 2])

    with left:
        st.subheader("Live vitals")
        pid = st.selectbox("Patient", sorted(vitals["patient_id"].unique()), key="pid")
        metric = st.selectbox("Metric", ["heart_rate", "systolic_bp", "diastolic_bp",
                                         "temperature", "respiration_rate", "spo2"], key="metric")
        pv = vitals[vitals["patient_id"] == pid].sort_values("event_time").tail(150)
        st.line_chart(pv.set_index("event_time")[metric])

    with right:
        st.subheader("Latest reading per patient")
        latest = (vitals.sort_values("event_time").groupby("patient_id").tail(1)
                  .set_index("patient_id").drop(columns=["sim_time", "event_time"]))
        st.dataframe(latest.round(1), use_container_width=True, height=300)

    st.subheader("Recent alerts")
    if alerts.empty:
        st.write("No alerts yet.")
    else:
        a, b = st.columns([3, 2])
        with a:
            cols = ["event_time", "patient_name", "admitted_ward", "metric", "direction",
                    "value", "normal_low", "normal_high", "severity"]
            st.dataframe(alerts[[c for c in cols if c in alerts.columns]].iloc[::-1].head(100),
                         use_container_width=True, height=330)
        with b:
            st.caption("Alerts by metric")
            st.bar_chart(alerts["metric"].value_counts())


live()


@st.fragment(run_every=15)
def storage():
    st.divider()
    st.subheader("Stored in HDFS")
    c1, c2 = st.columns(2)
    for col, label, path in ((c1, "Vitals (parquet)", "/patient_vitals/vitals"),
                             (c2, "Alerts (csv)", "/patient_vitals/alerts")):
        s = hdfs_summary(path)
        with col:
            if s:
                st.metric(label, f"{s['length'] / 1024:,.0f} KB", f"{s['fileCount']} files")
            else:
                st.metric(label, "n/a", "not written yet")
    hist = hdfs_read_alerts()
    if not hist.empty:
        st.caption(f"Alert history read back from HDFS ({len(hist)} rows from the newest files)")
        h1, h2 = st.columns(2)
        with h1:
            st.caption("Alerts by admitted ward (x: ward number, y: alert count)")
            st.bar_chart(hist["admitted_ward"].astype(str).value_counts())
        with h2:
            st.caption("Alerts by severity (CRITICAL vs WARNING)")
            st.bar_chart(hist["severity"].value_counts())


storage()


def hbase_lookup(pid, n_alerts=10):
    """Point lookup by patient id: the access pattern HBase is built for."""
    import happybase
    t0 = time.perf_counter()
    conn = happybase.Connection(os.environ.get("HBASE_HOST", "hbase"), 9090, timeout=5000)
    try:
        key = f"{int(pid):05d}".encode()
        latest = {k.decode().split(":")[1]: v.decode()
                  for k, v in conn.table("latest_vitals").row(key).items()}
        alerts = [{k.decode().split(":")[1]: v.decode() for k, v in d.items()}
                  for _, d in conn.table("alerts").scan(row_prefix=key + b"#", limit=n_alerts)]
    finally:
        conn.close()
    return latest, alerts, (time.perf_counter() - t0) * 1000


st.divider()
st.subheader("Patient lookup (HBase)")
st.caption("A doctor pulls up one patient instantly: a key lookup in HBase, no scan of the full history.")
lp = st.number_input("Patient id", min_value=1, max_value=10, value=1, step=1)
if st.button("Look up in HBase"):
    try:
        latest, alerts, ms = hbase_lookup(lp)
        st.success(f"Answered in {ms:.0f} ms")
        c1, c2 = st.columns(2)
        with c1:
            st.write("**Latest vitals**")
            st.json(latest)
        with c2:
            st.write("**Most recent alerts**")
            cols = ["metric", "direction", "value", "severity", "patient_name", "admitted_ward"]
            st.dataframe(pd.DataFrame(alerts)[[c for c in cols if alerts and c in alerts[0]]]
                         if alerts else pd.DataFrame({"info": ["no alerts for this patient yet"]}),
                         use_container_width=True)
    except Exception as e:
        st.error(f"HBase not reachable yet: {e}")
