"""Simple live dashboard: Kafka (live vitals + alerts) and HDFS (stored data)."""
import io
import json
import os
import threading
import time
import uuid
from collections import deque
from datetime import datetime

import pandas as pd
import requests
import streamlit as st
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
WEBHDFS = os.environ.get("WEBHDFS", "http://namenode:9870/webhdfs/v1")
ALERTS_DIR = os.environ.get("HDFS_ALERTS_DIR", "/patient_vitals/alerts")

st.set_page_config(page_title="Patient Vitals Monitor", layout="wide")


@st.cache_resource
def start_consumers():
    """Background threads keep the latest messages of both topics in memory."""
    store = {"vitals": deque(maxlen=3000), "alerts": deque(maxlen=1000),
             "vitals_total": 0, "alerts_total": 0, "demo_received_at": {}}

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
            if key == "alerts" and str(msg.value.get("sim_time", "")).startswith("DEMO-"):
                store["demo_received_at"][msg.value["sim_time"]] = time.time()

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
st.caption("Live patient monitoring, streaming analytics, and fast patient lookup")
live_tab, demo_tab, architecture_tab = st.tabs(
    ["Live Monitor", "Demo Lab", "System Architecture"]
)


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


def publish_demo_reading(patient_id):
    """Publish one clearly tagged synthetic reading into the normal processing path."""
    now = datetime.now()
    event_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    demo_id = f"DEMO-{now.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
    message = {
        "patient_id": int(patient_id),
        "heart_rate": 160.0,
        "systolic_bp": 120.0,
        "diastolic_bp": 80.0,
        "temperature": 98.6,
        "respiration_rate": 16.0,
        "spo2": 98.0,
        "sim_time": demo_id,
        "event_time": event_time,
    }
    producer = KafkaProducer(
        bootstrap_servers=KAFKA,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: str(key).encode("utf-8"),
        max_block_ms=5000,
        request_timeout_ms=5000,
    )
    try:
        producer.send("patient-vitals", key=str(patient_id), value=message).get(timeout=8)
    finally:
        producer.close(timeout=5)
    return demo_id, event_time, time.time()


with live_tab:
    st.caption("Kafka live feed refreshes every 3 seconds. Stored HDFS summaries refresh every 15 seconds.")
    live()
    storage()
    st.divider()
    st.subheader("Patient lookup (HBase)")
    st.caption("Fetch one patient's latest reading and recent alerts by key, without scanning the full history.")
    lp = st.number_input("Patient ID", min_value=1, max_value=10, value=1, step=1, key="lookup_patient_id")
    if st.button("Look up in HBase", key="lookup_hbase"):
        try:
            latest, alerts, ms = hbase_lookup(lp)
            st.success(f"Lookup completed in {ms:.0f} ms")
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


@st.fragment(run_every=2)
def demo_lab():
    st.subheader("Create a live demo alert")
    st.write(
        "Send one tagged synthetic reading through Kafka. Spark processes it with the same "
        "five-second micro-batch and threshold rules as the live feed; the alert then appears "
        "here and in Live Monitor."
    )
    info, action = st.columns([3, 2], vertical_alignment="center")
    with info:
        patient_id = st.selectbox("Patient for demo scenario", list(range(1, 11)), key="demo_patient_id")
        st.caption("Scenario: heart rate 160 bpm (configured normal range 50–110 bpm). Other vitals stay normal.")
    with action:
        st.metric("Expected result", "1 CRITICAL alert", "Heart rate is 45.5% above the upper limit")
        clicked = st.button("Generate Demo Alert", type="primary", use_container_width=True, key="generate_demo_alert")
    if clicked:
        try:
            demo_id, event_time, published_at = publish_demo_reading(patient_id)
            st.session_state["demo_run_id"] = demo_id
            st.session_state["demo_event_time"] = event_time
            st.session_state["demo_published_at"] = published_at
            st.toast("Demo reading published to Kafka", icon="🚨")
        except Exception as e:
            st.error(f"Could not publish the demo reading to Kafka: {e}")

    demo_id = st.session_state.get("demo_run_id")
    if not demo_id:
        st.info("Press Generate Demo Alert to start the end-to-end scenario.")
        return

    st.caption(f"Run ID: `{demo_id}` · Event time: {st.session_state.get('demo_event_time', '')}")
    found = [a for a in list(store["alerts"]) if a.get("sim_time") == demo_id]
    stage1, stage2, stage3 = st.columns(3)
    stage1.success("1 · Published to Kafka")
    if found:
        stage2.success("2 · Alert created by Spark")
        stage3.success("3 · Received by dashboard")
        received_at = store["demo_received_at"].get(demo_id)
        published_at = st.session_state.get("demo_published_at")
        if received_at and published_at:
            st.metric("Button-to-alert time", f"{max(0, received_at - published_at):.1f} s")
        st.dataframe(pd.DataFrame(found)[[
            c for c in ["patient_id", "metric", "direction", "value", "normal_low",
                        "normal_high", "severity", "sim_time"] if c in found[0]
        ]], use_container_width=True, hide_index=True)
        st.caption("This tagged synthetic event also follows the normal HDFS, HBase, and Hive paths.")
    else:
        stage2.info("2 · Waiting for Spark micro-batch")
        stage3.info("3 · Waiting for alert topic")
        st.caption("The alert should appear after Spark's next processing trigger (about five seconds, plus system load).")


architecture_dot = r"""digraph PatientVitals {
  graph [rankdir=LR, splines=ortho, nodesep=0.48, ranksep=0.72, pad=0.25, bgcolor="transparent"];
  node [shape=box, style="rounded,filled", fontname="Arial", fontsize=11, color="#B9CAD6", penwidth=1.2, margin="0.20,0.13"];
  edge [fontname="Arial", fontsize=9, color="#58788F", penwidth=1.5, arrowsize=0.75];

  csv [label="Synthetic Vitals CSV\n43,200 readings / 10 patients", fillcolor="#E6F2F8", color="#3A8FB7"];
  producer [label="Producer\nCSV replay -> JSON", fillcolor="#E6F2F8", color="#3A8FB7"];
  input [label="Kafka: patient-vitals\nKeyed by patient ID", fillcolor="#E6F2F8", color="#3A8FB7"];
  spark [label="Spark Structured Streaming\n5-second micro-batches\nParse -> threshold rules -> enrich", fillcolor="#17324D", fontcolor="white", color="#17324D", penwidth=1.8];
  hdfs [label="HDFS\nVitals: date-partitioned Parquet\nAlerts: CSV", fillcolor="#E7F3F1", color="#238C86"];
  hive [label="Hive\nExternal SQL tables\nMapReduce query engine", fillcolor="#E7F3F1", color="#238C86"];
  output [label="Kafka: patient-alerts\nWARNING / CRITICAL", fillcolor="#FFF3DF", color="#D8922E"];
  hbase_writer [label="HBase Writer\nConsumes both Kafka topics", fillcolor="#F1EDF8", color="#8C73B7"];
  hbase [label="HBase\nLatest vitals + alert history", fillcolor="#F1EDF8", color="#8C73B7"];
  dashboard [label="Streamlit Dashboard\nLive monitor + alert history\nPatient lookup", fillcolor="#EEF3F7", color="#58788F", penwidth=1.8];

  csv -> producer -> input -> spark;
  spark -> hdfs [label="Parquet + CSV"];
  hdfs -> hive [label="SQL analytics"];
  spark -> output [label="JSON alerts"];
  input -> hbase_writer [label="independent consumer"];
  output -> hbase_writer;
  hbase_writer -> hbase;
  input -> dashboard [label="live vitals", constraint=false];
  output -> dashboard [label="live alerts", constraint=false];
  hdfs -> dashboard [label="WebHDFS", constraint=false];
  hbase -> dashboard [label="point lookup", constraint=false];
}
"""

with demo_tab:
    demo_lab()

with architecture_tab:
    st.subheader("How the system works")
    st.write("The live pipeline starts with simulated monitor readings, evaluates them in Spark, and serves them through three storage paths.")
    st.graphviz_chart(architecture_dot, use_container_width=True)
    a, b, c = st.columns(3)
    a.markdown("**Stream processing**\n\nKafka buffers readings; Spark parses and checks each five-second batch.")
    b.markdown("**History and analytics**\n\nHDFS stores Parquet vitals and CSV alerts. Hive exposes SQL tables over those files.")
    c.markdown("**Fast patient view**\n\nHBase keeps the latest reading and alert history for quick patient lookups.")
    st.caption("The demo runs locally in Docker Compose. Spark uses local[2], Kafka has one partition, and HDFS is single-node.")
