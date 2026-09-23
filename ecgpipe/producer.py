"""Real-time ingestion: every patient is a simulated wearable that publishes one JSON message
per second of ECG to Kafka. The message key is the patient id, so each patient's chunks land on
the same partition and keep their order.

    python -m ecgpipe.producer [--speed 5] [--start-min 0] [--minutes 30]
"""
import argparse
import json
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from . import config, dataset


def ensure_topic(topic, bootstrap=config.KAFKA_BOOTSTRAP):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic not in admin.list_topics(timeout=5).topics:
        admin.create_topics([NewTopic(topic, num_partitions=config.KAFKA_PARTITIONS)])[topic].result()


def run(records=config.RECORDS, speed=5.0, start_s=0, seconds=None, topic=config.KAFKA_TOPIC):
    ensure_topic(topic)
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "linger.ms": 5})
    signals = {r: dataset.load_signal(r) for r in records}
    fs = next(iter(signals.values()))[0]
    last_s = min(len(sig) for _, _, sig in signals.values()) // fs
    end_s = last_s if seconds is None else min(last_s, start_s + seconds)

    t0, sent = time.time(), 0
    try:
        for i, t in enumerate(range(start_s, end_s)):
            for rid, (_, leads, sig) in signals.items():
                chunk = sig[t * fs:(t + 1) * fs]
                msg = {"patient_id": rid, "t": t, "fs": fs, "sent_at": time.time(),
                       "signal": {lead: chunk[:, j].round(3).tolist() for j, lead in enumerate(leads)}}
                producer.produce(topic, key=rid, value=json.dumps(msg))
                sent += 1
            producer.poll(0)
            if t % 10 == 0:
                print(f"t={t:>5}s  sent={sent}", flush=True)
            time.sleep(max(0.0, t0 + (i + 1) / speed - time.time()))  # pace: `speed` seconds of ECG per second
    except KeyboardInterrupt:
        pass
    producer.flush(10)
    print(f"done: {sent} messages in {time.time() - t0:.1f}s")
    return sent


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--speed", type=float, default=5, help="replay speed-up (1 = real time)")
    p.add_argument("--start-min", type=float, default=0, help="where to start in the 2-hour recordings")
    p.add_argument("--minutes", type=float, default=None, help="how much ECG to stream (default: all)")
    a = p.parse_args()
    run(speed=a.speed, start_s=int(a.start_min * 60), seconds=None if a.minutes is None else int(a.minutes * 60))
