"""Demo UI: reads only from MongoDB (the serving layer) and refreshes itself twice a second.

Theme (dark, rounded, fonts, status colors) lives in .streamlit/config.toml; the small CSS block
below only adds what theming cannot: the gradient backdrop, gradient cards and the selection glow.

    streamlit run dashboard.py
"""
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from ecgpipe import config
from ecgpipe.storage import get_db

BLUE, ORANGE, INK_2 = "#3987e5", "#d95926", "#c3c2b7"
STATUS = {  # label, Material icon, theme color: a status is never shown by color alone
    "normal": ("Normal", ":material/favorite:", "green"),
    "brady": ("Bradycardia", ":material/arrow_downward:", "orange"),
    "tachy": ("Tachycardia", ":material/arrow_upward:", "red"),
    "unknown": ("Waiting", ":material/hourglass_empty:", "gray"),
}
CARD = "linear-gradient(150deg, rgba(255,255,255,.07), rgba(255,255,255,.015))"

st.set_page_config(page_title="ECG stream monitor", page_icon=":material/monitor_heart:", layout="wide")
db = st.cache_resource(get_db)()
patients = {p["_id"]: p for p in db.patients.find().sort("_id")}

st.title("ECG stream monitor", icon=":material/monitor_heart:")
st.caption("Real-time cardiac telemonitoring of 5 patients · wearables → Kafka → Python stream processor → MongoDB")
if not patients:
    st.warning("No patients yet: run `python -m ecgpipe.ingest_batch` first.")
    st.stop()
pid = st.segmented_control("Patient to inspect", list(patients), default=next(iter(patients))) or next(iter(patients))

st.html(f"""<style>
[data-testid="stApp"] {{ background:
  radial-gradient(900px 500px at 0% -10%, rgba(57,135,229,.20), transparent 60%),
  radial-gradient(700px 450px at 100% 0%, rgba(144,133,233,.14), transparent 60%), #0b0c10; }}
[data-testid="stHeader"] {{ background: transparent; }}
.block-container {{ max-width: 1060px; }}
[class*="st-key-pt_"], .st-key-card_ecg, .st-key-card_hr {{ background: {CARD}; }}
.st-key-pt_{pid} {{ border-color: rgba(57,135,229,.8) !important;
  box-shadow: 0 0 0 1px rgba(57,135,229,.35), 0 8px 30px rgba(57,135,229,.2); }}
</style>""")


def chart(fig, height, y_title, y_range=None):
    fig.update_layout(
        height=height, margin=dict(l=58, r=10, t=34, b=46), hovermode="x unified",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font=dict(family="Inter", color=INK_2, size=12),
        legend=dict(orientation="h", x=0, y=1.02, yanchor="bottom", font=dict(size=12.5, color="#e8e7e2")),
        hoverlabel=dict(bgcolor="#16181e", bordercolor="#262a33", font=dict(color="#fff")))
    axis = dict(zeroline=False, color=INK_2, automargin=True, title_font=dict(size=12, color="#8f8e88"))
    fig.update_xaxes(title="recording time (s)", showgrid=False, **axis)
    fig.update_yaxes(title=y_title, gridcolor="rgba(255,255,255,.06)", range=y_range, **axis)
    st.plotly_chart(fig, width="stretch", theme=None, config={"displayModeBar": False})


@st.fragment(run_every=0.5)
def live():
    # pipeline health
    recent = list(db.ecg_windows.find({"ts": {"$gte": datetime.now(timezone.utc) - timedelta(seconds=3)}},
                                      {"latency_ms": 1}))
    with st.container(horizontal=True, gap="small"):
        if recent:
            st.badge("Live", icon=":material/sensors:", color="green")
            st.badge(f"{len(recent) / 3:.0f} messages / s", icon=":material/speed:", color="gray")
            st.badge(f"{np.median([r['latency_ms'] for r in recent]):.0f} ms device → database",
                     icon=":material/timer:", color="gray")
            st.badge(f"{db.alerts.count_documents({})} alerts raised", icon=":material/notifications_active:",
                     color="gray")
        else:
            st.badge("Idle · waiting for the stream", icon=":material/hourglass_empty:", color="gray")

    # one card per patient: latest heart rate + its status
    with st.container(horizontal=True, gap="small"):
        for rid in patients:
            w = db.ecg_windows.find_one({"patient_id": rid}, {"hr": 1, "hr_class": 1}, sort=[("ts", -1)])
            label, icon, color = STATUS[w["hr_class"] if w else "unknown"]
            with st.container(border=True, key=f"pt_{rid}"):
                st.metric(f"Patient {rid}", f"{w['hr']:.0f} bpm" if w and w["hr"] else "–")
                st.badge(label, icon=icon, color=color)
    st.caption(":green-badge[:material/favorite: Normal] 60–100 bpm · "
               ":orange-badge[:material/arrow_downward: Bradycardia] under 60 · "
               ":red-badge[:material/arrow_upward: Tachycardia] over 100 · an alert fires after 5 s in one status")

    # live ECG of the selected patient
    windows = sorted(db.ecg_windows.find({"patient_id": pid}, sort=[("ts", -1)], limit=config.CONTEXT_S),
                     key=lambda w: w["t"])
    p = patients[pid]
    with st.container(border=True, key="card_ecg"):
        st.subheader(f"Live ECG · patient {pid}", icon=":material/ecg_heart:")
        st.caption(f"The heart's electrical signal over the last {config.CONTEXT_S} seconds; each orange dot is a "
                   f"heartbeat found by the stream processor. {p.get('age', '?')} y {p.get('sex', '')} · "
                   f"{', '.join(p['diagnoses'])} · {', '.join(p['medications'])}")
        fig = go.Figure()
        if windows:
            y = np.concatenate([w["samples"] for w in windows])
            x = windows[0]["t"] + np.arange(len(y)) / windows[0]["fs"]
            beats = [b for w in windows for b in w["beats"] if x[0] <= b <= x[-1]]
            fig.add_trace(go.Scatter(x=x, y=y, mode="lines", hoverinfo="skip", showlegend=False,
                                     line=dict(color="rgba(57,135,229,.25)", width=7)))  # soft glow
            fig.add_trace(go.Scatter(x=x, y=y, mode="lines", name=f"ECG signal, lead {windows[0]['lead']} (mV)",
                                     line=dict(color=BLUE, width=1.6)))
            fig.add_trace(go.Scatter(x=beats, y=np.interp(beats, x, y), mode="markers", name="Detected heartbeat",
                                     marker=dict(size=9, color=ORANGE, line=dict(width=2, color="#16181e"))))
        chart(fig, 250, "millivolts")

    # heart-rate trend, aggregated in MongoDB
    with st.container(border=True, key="card_hr"):
        st.subheader("Heart-rate trend · last 5 minutes", icon=":material/show_chart:")
        st.caption("Average heart rate every 10 seconds, computed in MongoDB. Inside the green band the rhythm is "
                   "normal; below it is bradycardia, above it tachycardia.")
        trend = pd.DataFrame(db.ecg_windows.aggregate([
            {"$match": {"patient_id": pid, "t": {"$gte": (windows[-1]["t"] if windows else 0) - 300},
                        "hr": {"$ne": None}}},
            {"$group": {"_id": {"$subtract": ["$t", {"$mod": ["$t", 10]}]}, "hr": {"$avg": "$hr"}}},
            {"$sort": {"_id": 1}},
        ]))
        fig = go.Figure()
        if not trend.empty:
            x0, x1 = trend["_id"].min(), max(trend["_id"].max(), trend["_id"].min() + 10)
            fig.add_trace(go.Scatter(x=[x0, x1, x1, x0], y=[config.BRADY_BPM] * 2 + [config.TACHY_BPM] * 2,
                                     fill="toself", mode="none", fillcolor="rgba(12,163,12,.18)", hoverinfo="skip",
                                     name=f"Normal range {config.BRADY_BPM}–{config.TACHY_BPM} bpm"))
            fig.add_trace(go.Scatter(x=trend["_id"], y=trend["hr"].round(1), mode="lines", name="Heart rate (bpm)",
                                     line=dict(color=BLUE, width=2.5, shape="spline"), fill="tozeroy",
                                     fillgradient=dict(type="vertical", colorscale=[[0, "rgba(57,135,229,0)"],
                                                                                    [1, "rgba(57,135,229,.35)"]])))
        chart(fig, 200, "beats / min", y_range=[35, 115])


live()
