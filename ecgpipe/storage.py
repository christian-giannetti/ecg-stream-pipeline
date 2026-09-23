"""MongoDB operational store.

Collections
  patients     one document per patient, loaded in batch from the WFDB headers
  ecg_windows  bucket pattern: one document = one patient x one second of cleaned ECG + derived HR
  alerts       sustained brady/tachycardia episodes detected on the stream
"""
from pymongo import ASCENDING, MongoClient

from . import config


def get_db(name=config.MONGO_DB, uri=config.MONGO_URI):
    return MongoClient(uri, serverSelectionTimeoutMS=3000, tz_aware=True)[name]


def ensure_indexes(db):
    # Serves the most frequent query ("latest windows of patient X") and makes stream writes
    # idempotent: a message re-delivered by Kafka overwrites its own window instead of duplicating it.
    db.ecg_windows.create_index([("patient_id", ASCENDING), ("t", ASCENDING)], unique=True)
    db.ecg_windows.create_index("ts")
    db.alerts.create_index([("patient_id", ASCENDING), ("t", ASCENDING)], unique=True)
