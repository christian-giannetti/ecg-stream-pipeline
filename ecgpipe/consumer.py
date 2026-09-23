"""Stream processing: consume ECG chunks, preprocess them, enrich with patient data, store.

For every message: clean lead 0 (cleaning), keep only that lead (reduction), detect beats and
derive HR (transformation), label it brady/normal/tachy (discretization), add age/sex from the
batch-loaded patient document (integration, join done at write time), upsert into MongoDB.
Kafka offsets are committed only after the write succeeds (at-least-once), and the unique
(patient_id, t) index makes re-deliveries idempotent.

    python -m ecgpipe.consumer
"""
import argparse
import json
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer
from pymongo import UpdateOne

from . import config
from .processing import PatientStream
from .storage import ensure_indexes, get_db

ALERT_AFTER_S = 5   # an abnormal class must last this long before it raises an alert


class Patient:
    """Everything the consumer remembers about one patient between messages."""

    def __init__(self, fs, profile):
        self.fs, self.stream, self.next_t = fs, PatientStream(fs), None
        self.profile = {k: profile.get(k) for k in ("age", "sex")}
        self.state, self.candidate, self.run = "unknown", None, 0

    def push(self, samples, t):
        """Feed one window. A jump in time (device restarted, stream replayed) starts a fresh session,
        otherwise the filter and beat state of the old session would corrupt the new one."""
        if t != self.next_t:
            self.stream = PatientStream(self.fs)
        self.next_t = t + 1
        return self.stream.push(samples, t * self.fs)

    def alert(self, hr_class):
        """Hysteresis: report a new abnormal state only once it has been stable for ALERT_AFTER_S."""
        self.candidate, self.run = hr_class, (self.run + 1 if hr_class == self.candidate else 1)
        if self.run == ALERT_AFTER_S and hr_class != self.state and hr_class != "unknown":
            self.state = hr_class
            return hr_class in ("brady", "tachy")
        return False


def to_documents(msg, patient):
    lead = next(iter(msg["signal"]))
    fs, t = msg["fs"], msg["t"]
    res = patient.push(msg["signal"][lead], t)
    now = time.time()
    window = {
        "patient_id": msg["patient_id"], "t": t, "lead": lead, "fs": fs,
        "samples": res.clean.round(3).tolist(),
        "beats": [round(b / fs, 3) for b in res.beats],
        "hr": res.hr, "hr_class": res.hr_class,
        "patient": patient.profile,
        "ts": datetime.fromtimestamp(now, timezone.utc),
        "latency_ms": round((now - msg["sent_at"]) * 1000, 1),
    }
    alert = None
    if patient.alert(res.hr_class):
        alert = {"patient_id": msg["patient_id"], "t": t, "type": res.hr_class, "hr": res.hr,
                 "ts": window["ts"]}
    return window, alert


def run(topic=config.KAFKA_TOPIC, group=config.KAFKA_GROUP, db=None, idle_timeout=None):
    """Consume until interrupted, or until no message arrives for `idle_timeout` seconds."""
    db = db if db is not None else get_db()
    ensure_indexes(db)
    consumer = Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "group.id": group,
                         "auto.offset.reset": "earliest", "enable.auto.commit": False,
                         "session.timeout.ms": 10000})  # a crashed consumer is replaced after 10 s
    patients, stored, last_msg = {}, 0, float("inf")  # idle clock starts once partitions are assigned

    def on_assign(c, partitions):
        nonlocal last_msg
        last_msg = time.time()

    consumer.subscribe([topic], on_assign=on_assign)
    try:
        while idle_timeout is None or time.time() - last_msg < idle_timeout:
            batch = [m for m in consumer.consume(num_messages=200, timeout=0.25) if not m.error()]
            if not batch:
                continue
            last_msg = time.time()
            windows, alerts = [], []
            for m in batch:
                msg = json.loads(m.value())
                pid = msg["patient_id"]
                if pid not in patients:
                    patients[pid] = Patient(msg["fs"], db.patients.find_one({"_id": pid}) or {})
                window, alert = to_documents(msg, patients[pid])
                windows.append(UpdateOne({"patient_id": pid, "t": window["t"]}, {"$set": window}, upsert=True))
                if alert:
                    alerts.append(UpdateOne({"patient_id": pid, "t": alert["t"]}, {"$set": alert}, upsert=True))
                    print(f"ALERT {pid} t={alert['t']}s {alert['type']} hr={alert['hr']}", flush=True)
            db.ecg_windows.bulk_write(windows, ordered=False)
            if alerts:
                db.alerts.bulk_write(alerts, ordered=False)
            consumer.commit(asynchronous=False)
            stored += len(windows)
            print(f"stored {stored} windows", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    return stored


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal.default_int_handler)  # `kill` = Ctrl+C: leave the group cleanly
    argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter).parse_args()
    run()
