"""Access to the PhysioNet European ST-T Database (download, headers, signals, annotations)."""
import re

import numpy as np
import wfdb

from . import config

BEAT_SYMBOLS = set("NLRBAaJSVrFejnE/fQ?")  # WFDB symbols that mark a heartbeat


def download(records=config.RECORDS, data_dir=config.DATA_DIR):
    """Fetch .hea/.dat/.atr for the selected records (skips files already present)."""
    missing = [r for r in records if not (data_dir / f"{r}.dat").exists()]
    if missing:
        data_dir.mkdir(parents=True, exist_ok=True)
        wfdb.dl_database(config.PHYSIONET_DB, str(data_dir), records=missing, annotators=["atr"])
    return records


def parse_header(record_id, data_dir=config.DATA_DIR):
    """Turn a WFDB header (semi-structured free text) into a patient document."""
    h = wfdb.rdheader(str(data_dir / record_id))
    doc = {"_id": record_id, "fs": h.fs, "leads": h.sig_name,
           "duration_s": h.sig_len / h.fs, "diagnoses": [], "medications": []}
    for line in (c.strip() for c in h.comments):
        if m := re.match(r"Age:\s*(\d+)\s+Sex:\s*(\w)", line):
            doc["age"], doc["sex"] = int(m[1]), m[2]
        elif line.startswith("Medications:"):
            doc["medications"] = [x.strip() for x in line.split(":", 1)[1].split(",") if x.strip()]
        elif line.startswith("Recorder type:"):
            doc["recorder"] = line.split(":", 1)[1].strip()
        elif line:
            doc["diagnoses"].append(line)
    return doc


def load_signal(record_id, data_dir=config.DATA_DIR):
    """Return (fs, lead names, signal in mV with shape [n_samples, n_leads])."""
    rec = wfdb.rdrecord(str(data_dir / record_id))
    return rec.fs, rec.sig_name, rec.p_signal.astype(np.float32)


def reference_beats(record_id, data_dir=config.DATA_DIR):
    """Sample indices of the cardiologist-annotated beats (ground truth for veracity checks)."""
    ann = wfdb.rdann(str(data_dir / record_id), "atr")
    return np.array([s for s, sym in zip(ann.sample, ann.symbol) if sym in BEAT_SYMBOLS])
