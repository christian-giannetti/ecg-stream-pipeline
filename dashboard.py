"""Demo UI: reads only from MongoDB (the serving layer) and refreshes itself every second.

    streamlit run dashboard.py
"""
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from ecgpipe import config
from ecgpipe.storage import get_db

SERIES, MUTED = "#2a78d6", "#8a8984"
STATUS = {"normal": ":green[● normal]", "brady": ":orange[▼ brady]",
          "tachy": ":red[▲ tachy]", "unknown": ":gray[○ no signal]"}

st.set_page_config(page_title="ECG stream monitor", layout="wide")
db = st.cache_resource(get_db)()


def chart(fig, height):
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=10, b=10), hovermode="x unified", showlegend=False)
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(128,128,128,0.15)")
    st.plotly_chart(fig, width="stretch", theme="streamlit")


patients = {p["_id"]: p for p in db.patients.find().sort("_id")}
if not patients:
    st.warning("No patients yet: run `python -m ecgpipe.ingest_batch` first.")
    st.stop()

with st.sidebar:
    st.header("Patient")
    pid = st.radio("Patient", list(patients), label_visibility="collapsed")
    p = patients[pid]
    st.markdown(f"**{p.get('age', '?')} y, {p.get('sex', '?')}** · leads {', '.join(p['leads'])}")
    st.markdown("**Diagnoses**  \n" + "  \n".join(p["diagnoses"]) if p["diagnoses"] else "")
    st.markdown("**Medications**  \n" + ", ".join(p["medications"]) if p["medications"] else "")
    if q := p.get("quality"):
        st.caption(f"Veracity (first {q['minutes']:g} min vs. cardiologist annotations): "
                   f"sensitivity {q['sensitivity']:.1%}, PPV {q['ppv']:.1%}")
    st.caption("Batch layer: loaded from the WFDB headers by `ingest_batch`.")

st.title("ECG stream monitor")
st.caption("simulated wearables → Kafka `ecg.raw` → stream processor → MongoDB → this page")


@st.fragment(run_every="1s")
def live():
    now = datetime.now(timezone.utc)
    recent = list(db.ecg_windows.find({"ts": {"$gte": now - timedelta(seconds=5)}}, {"latency_ms": 1}))
    c = st.columns(4)
    c[0].metric("Windows stored", f"{db.ecg_windows.estimated_document_count():,}")
    c[1].metric("Ingest rate", f"{len(recent) / 5:.0f} msg/s")
    c[2].metric("Median latency", f"{np.median([r['latency_ms'] for r in recent]):.0f} ms" if recent else "–")
    c[3].metric("Alerts", db.alerts.estimated_document_count())

    # one tile per patient: latest HR and its discretized class
    for col, rid in zip(st.columns(len(patients)), patients):
        w = db.ecg_windows.find_one({"patient_id": rid}, {"hr": 1, "hr_class": 1, "t": 1}, sort=[("t", -1)])
        col.metric(rid, f"{w['hr']:.0f} bpm" if w and w["hr"] else "–")
        col.markdown(STATUS[w["hr_class"]] if w else STATUS["unknown"])

    windows = list(db.ecg_windows.find({"patient_id": pid}, sort=[("t", -1)], limit=config.CONTEXT_S))[::-1]
    if not windows:
        st.info("Waiting for the stream… start `python -m ecgpipe.producer`.")
        return
    fs = windows[0]["fs"]
    y = np.concatenate([w["samples"] for w in windows])
    x = windows[0]["t"] + np.arange(len(y)) / fs
    beats = [b for w in windows for b in w["beats"] if x[0] <= b <= x[-1]]

    st.subheader(f"{pid} · live ECG ({windows[0]['lead']}, cleaned)")
    fig = go.Figure(go.Scatter(x=x, y=y, mode="lines", line=dict(color=SERIES, width=1.5), name="mV"))
    fig.add_trace(go.Scatter(x=beats, y=np.interp(beats, x, y), mode="markers", name="beat",
                             marker=dict(size=8, color=SERIES, line=dict(width=2, color="white"))))
    fig.update_xaxes(title="recording time (s)")
    fig.update_yaxes(title="mV")
    chart(fig, 280)

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Heart rate, last 5 min (10-s buckets)")
        trend = pd.DataFrame(db.ecg_windows.aggregate([
            {"$match": {"patient_id": pid, "t": {"$gte": windows[-1]["t"] - 300}, "hr": {"$ne": None}}},
            {"$group": {"_id": {"$subtract": ["$t", {"$mod": ["$t", 10]}]}, "hr": {"$avg": "$hr"}}},
            {"$sort": {"_id": 1}},
        ]))
        fig = go.Figure()
        fig.add_hrect(y0=config.BRADY_BPM, y1=config.TACHY_BPM, fillcolor=MUTED, opacity=0.1, line_width=0)
        if not trend.empty:
            fig.add_trace(go.Scatter(x=trend["_id"], y=trend["hr"].round(1), mode="lines",
                                     line=dict(color=SERIES, width=2), name="bpm"))
        fig.update_xaxes(title="recording time (s)")
        fig.update_yaxes(title="bpm")
        chart(fig, 240)
        st.caption(f"Shaded band = normal range ({config.BRADY_BPM}–{config.TACHY_BPM} bpm).")
    with right:
        st.subheader("Alerts")
        alerts = list(db.alerts.find({}, {"_id": 0, "patient_id": 1, "t": 1, "type": 1, "hr": 1},
                                     sort=[("ts", -1)], limit=8))
        if alerts:
            st.dataframe(pd.DataFrame(alerts), hide_index=True, width="stretch")
        else:
            st.caption("No sustained brady/tachycardia so far.")


@st.fragment(run_every="5s")
def analytics():
    st.subheader("Descriptive analytics (MongoDB aggregation pipeline)")
    rows = list(db.ecg_windows.aggregate([
        {"$group": {"_id": "$patient_id", "minutes": {"$sum": 1 / 60}, "mean_hr": {"$avg": "$hr"},
                    "min_hr": {"$min": "$hr"}, "max_hr": {"$max": "$hr"},
                    **{f"% {c}": {"$avg": {"$cond": [{"$eq": ["$hr_class", c]}, 100, 0]}}
                       for c in ("brady", "normal", "tachy")}}},
        {"$sort": {"_id": 1}},
    ]))
    if rows:
        st.dataframe(pd.DataFrame(rows).rename(columns={"_id": "patient"}).round(1), hide_index=True, width="stretch")


live()
analytics()
