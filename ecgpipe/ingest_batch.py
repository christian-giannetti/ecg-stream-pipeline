"""Batch ingestion: download the records, turn the WFDB headers into patient documents and
attach a veracity report (our beat detector vs. the cardiologists' annotations).

    python -m ecgpipe.ingest_batch [--check-minutes 10]
"""
import argparse

import numpy as np

from . import config, dataset
from .processing import PatientStream, match_beats
from .storage import ensure_indexes, get_db


def veracity(record_id, minutes):
    """Replay the first minutes of a record through the *streaming* detector and score it."""
    fs, _, sig = dataset.load_signal(record_id)
    n = int(minutes * 60 * fs)
    stream, detected = PatientStream(fs), []
    for start in range(0, n, fs):
        detected += stream.push(sig[start:start + fs, 0], start).beats
    ref, detected, end = dataset.reference_beats(record_id), np.array(detected), n - fs  # same span on both sides
    sensitivity, ppv = match_beats(detected[detected < end], ref[ref < end], fs)
    return {"minutes": minutes, "sensitivity": round(sensitivity, 4), "ppv": round(ppv, 4)}


def run(records=config.RECORDS, check_minutes=10, db=None):
    db = db if db is not None else get_db()
    ensure_indexes(db)
    dataset.download(records)
    for rid in records:
        doc = dataset.parse_header(rid)
        doc["quality"] = veracity(rid, check_minutes)
        db.patients.replace_one({"_id": rid}, doc, upsert=True)
        q = doc["quality"]
        print(f"{rid}: {doc.get('age')}{doc.get('sex')}  {', '.join(doc['diagnoses'])[:48]:48}"
              f"  Se={q['sensitivity']:.1%}  PPV={q['ppv']:.1%}")
    return records


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--check-minutes", type=float, default=10, help="minutes per record used for the veracity check")
    run(check_minutes=p.parse_args().check_minutes)
