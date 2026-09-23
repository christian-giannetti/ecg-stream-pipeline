"""Demo UI: reads only from MongoDB (the serving layer) and refreshes itself twice a second.

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
          "tachy": ":red[▲ tachy]", "unknown": ":gray[○ waiting]"}

st.set_page_config(page_title="ECG stream monitor", layout="centered")
db = st.cache_resource(get_db)()


def chart(fig, height, x_title, y_title):
    fig.update_layout(height=height, margin=dict(l=0, r=0, t=0, b=0), showlegend=False, hovermode="x unified")
    fig.update_xaxes(title=x_title, showgrid=False)
    fig.update_yaxes(title=y_title, gridcolor="rgba(128,128,128,0.15)")
    st.plotly_chart(fig, width="stretch", theme="streamlit", config={"displayModeBar": False})


patients = {p["_id"]: p for p in db.patients.find().sort("_id")}
if not patients:
    st.warning("No patients yet: run `python -m ecgpipe.ingest_batch` first.")
    st.stop()

st.title("ECG stream monitor")
pid = st.segmented_control("Patient", list(patients), default=next(iter(patients)),
                           label_visibility="collapsed") or next(iter(patients))
p = patients[pid]
st.caption(f"{p.get('age', '?')} y {p.get('sex', '')} · {', '.join(p['diagnoses'])} · {', '.join(p['medications'])}")


@st.fragment(run_every=0.5)
def live():
    now = datetime.now(timezone.utc)
    recent = list(db.ecg_windows.find({"ts": {"$gte": now - timedelta(seconds=3)}}, {"latency_ms": 1}))
    if recent:
        st.markdown(f":green[● streaming] · {len(recent) / 3:.0f} msg/s · "
                    f"{np.median([r['latency_ms'] for r in recent]):.0f} ms latency · "
                    f"{db.alerts.count_documents({})} alerts")
    else:
        st.markdown(":gray[○ idle: start the producer]")

    for col, rid in zip(st.columns(len(patients)), patients):
        w = db.ecg_windows.find_one({"patient_id": rid}, {"hr": 1, "hr_class": 1}, sort=[("ts", -1)])
        col.metric(rid, f"{w['hr']:.0f} bpm" if w and w["hr"] else "–", border=True)
        col.markdown(STATUS[w["hr_class"] if w else "unknown"])

    windows = sorted(db.ecg_windows.find({"patient_id": pid}, sort=[("ts", -1)], limit=config.CONTEXT_S),
                     key=lambda w: w["t"])
    if not windows:
        return
    y = np.concatenate([w["samples"] for w in windows])
    x = windows[0]["t"] + np.arange(len(y)) / windows[0]["fs"]
    beats = [b for w in windows for b in w["beats"] if x[0] <= b <= x[-1]]
    fig = go.Figure(go.Scatter(x=x, y=y, mode="lines", line=dict(color=SERIES, width=1.5), name="mV"))
    fig.add_trace(go.Scatter(x=beats, y=np.interp(beats, x, y), mode="markers", name="beat",
                             marker=dict(size=8, color=SERIES, line=dict(width=2, color="white"))))
    chart(fig, 220, "seconds", f"{windows[0]['lead']} (mV)")

    trend = pd.DataFrame(db.ecg_windows.aggregate([
        {"$match": {"patient_id": pid, "t": {"$gte": windows[-1]["t"] - 300}, "hr": {"$ne": None}}},
        {"$group": {"_id": {"$subtract": ["$t", {"$mod": ["$t", 10]}]}, "hr": {"$avg": "$hr"}}},
        {"$sort": {"_id": 1}},
    ]))
    fig = go.Figure()
    fig.add_hrect(y0=config.BRADY_BPM, y1=config.TACHY_BPM, fillcolor=MUTED, opacity=0.12, line_width=0)
    if not trend.empty:
        fig.add_trace(go.Scatter(x=trend["_id"], y=trend["hr"].round(1), mode="lines",
                                 line=dict(color=SERIES, width=2), name="bpm"))
    chart(fig, 160, "seconds", "bpm")
    st.caption(f"Heart rate, last 5 min · shaded = normal range {config.BRADY_BPM}–{config.TACHY_BPM} bpm")


live()
