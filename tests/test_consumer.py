import time

from ecgpipe.consumer import Patient, to_documents
from tests.test_processing import FS, synthetic_ecg


def feed(patient, x, t0, seconds):
    docs = []
    for t in range(t0, t0 + seconds):
        msg = {"patient_id": "p1", "t": t, "fs": FS, "sent_at": time.time(),
               "signal": {"II": x[(t - t0) * FS:(t - t0 + 1) * FS].tolist()}}
        docs.append(to_documents(msg, patient)[0])
    return docs


def test_replayed_stream_starts_a_fresh_session():
    # the device streams t = 0..29, then restarts from t = 0 (e.g. a new demo run): beats must keep coming
    x, _ = synthetic_ecg(72, 30)
    patient = Patient(FS, {"age": 60, "sex": "F"})
    feed(patient, x, 0, 30)
    replay = feed(patient, x, 0, 30)
    assert sum(len(d["beats"]) for d in replay) >= 30
    assert abs(replay[-1]["hr"] - 72) < 3
