"""End-to-end: producer -> Kafka -> consumer -> MongoDB, on an isolated topic and database.
Skipped unless Kafka and MongoDB are running (scripts/services.sh start) and the data is downloaded."""
import socket
import uuid

import pytest

from ecgpipe import config, consumer, producer
from ecgpipe.storage import get_db

RECORDS, SECONDS = config.RECORDS[:2], 20


def _up(port):
    with socket.socket() as s:
        return s.connect_ex(("localhost", port)) == 0


pytestmark = pytest.mark.skipif(
    not (_up(9092) and _up(27017) and all((config.DATA_DIR / f"{r}.dat").exists() for r in RECORDS)),
    reason="needs Kafka, MongoDB and downloaded records")


@pytest.fixture
def isolated():
    run_id = uuid.uuid4().hex[:8]
    db = get_db(f"ecg_test_{run_id}")
    db.patients.insert_many([{"_id": r, "age": 60, "sex": "F"} for r in RECORDS])
    yield f"ecg.test.{run_id}", db
    db.client.drop_database(db.name)


def test_stream_lands_in_mongo_exactly_once(isolated):
    topic, db = isolated
    assert producer.run(RECORDS, speed=200, seconds=SECONDS, topic=topic) == len(RECORDS) * SECONDS

    consumer.run(topic=topic, group="g1", db=db, idle_timeout=5)
    assert db.ecg_windows.count_documents({}) == len(RECORDS) * SECONDS

    w = db.ecg_windows.find_one({"patient_id": RECORDS[0], "t": SECONDS - 1})
    assert len(w["samples"]) == 250 and w["patient"] == {"age": 60, "sex": "F"}  # reduced + enriched
    assert 40 < w["hr"] < 140 and w["hr_class"] in ("brady", "normal", "tachy")

    # replaying the whole topic (new consumer group) must not create duplicates
    consumer.run(topic=topic, group="g2", db=db, idle_timeout=5)
    assert db.ecg_windows.count_documents({}) == len(RECORDS) * SECONDS
