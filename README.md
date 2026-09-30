# ECG Stream Pipeline

A lean, end-to-end big-data pipeline for cardiology: **batch** clinical records and a **real-time** ECG stream are ingested,
preprocessed, stored in a document NoSQL database, and served on a live dashboard.

Built for the course *Big Data in Healthcare* (Prof. A. Celesti, University of Messina). It uses the PhysioNet
**European ST-T Database**, the same data as Carnevale, Celesti, Fazio, Villari, *"A Big Data Analytics Approach for the
Development of Advanced Cardiology Applications"*, Information 2020.

```
 [batch]  WFDB headers ──ingest_batch──► MongoDB.patients ─────────┐ enrich (join at write time)
                                                                  ▼
 [stream] producer ──► Kafka topic "ecg.raw" ──► consumer ──► MongoDB.ecg_windows / alerts ──► Streamlit dashboard
          5 simulated     key = patient_id       clean, detect beats,
          wearables,      3 partitions           HR, discretize, alert
          1 msg = 1 s ECG
```

## Highlights

- **Real-time ingestion** with Apache Kafka (KRaft, no ZooKeeper): 5 simulated wearables, 250 Hz × 2 leads, replayable up to N× real time.
- **Stream processing** in Python: causal band-pass filtering, R-peak detection, heart rate, brady/normal/tachy labels, sustained-episode alerts.
- **Document storage** in MongoDB using the bucket pattern (1 document = 1 patient × 1 second).
- **Exactly-once results** on top of at-least-once delivery: offsets are committed after the write, and a unique index makes re-deliveries idempotent.
- **Veracity check**: the beat detector is scored against the cardiologists' annotations (sensitivity / PPV).
- **Live dashboard** in Streamlit that reads only from MongoDB.
- **Zero Docker, zero system installs**: everything (Python, MongoDB, Java, Kafka) lives in one conda environment.

## Quick start

Requirements: macOS or Linux, [conda](https://conda-forge.org/download/) (Miniforge recommended), `curl`, `nc`.

```bash
git clone https://github.com/christian-giannetti/ecg-stream-pipeline.git
cd ecg-stream-pipeline
scripts/demo.sh          # speed 2x by default; scripts/demo.sh 1 = real time, scripts/demo.sh 10 = stress test
```

One command starts the services, loads the batch data, launches the stream processor and the producer, and opens the dashboard
at <http://localhost:8501>. Press **Ctrl+C** to stop everything.

On the first run, `scripts/setup.sh` creates the `ecgpipe` conda env (Python libraries, MongoDB 8, OpenJDK 17) and unpacks
Kafka 4.3 inside it. Later runs skip this step.

### Step by step

```bash
conda activate ecgpipe                      # after a first scripts/demo.sh (or scripts/setup.sh)
scripts/services.sh start                   # MongoDB :27017 + Kafka (KRaft) :9092
python -m ecgpipe.ingest_batch --reset      # download 5 records (~26 MB), load patients, run the veracity check
python -m ecgpipe.consumer                  # terminal 1: stream processor
streamlit run dashboard.py                  # terminal 2: http://localhost:8501
python -m ecgpipe.producer --speed 1        # terminal 3: 5 wearables in real time (1 msg/s each)
scripts/services.sh stop                    # data persists in $CONDA_PREFIX/var/ecgpipe (delete it to reset)
```

## How it works

| Stage | Implementation |
|---|---|
| Data generation (high **velocity**) | `producer.py` replays 5 two-hour recordings as live devices, one Kafka message per second of ECG |
| Batch ingestion (**variety**) | `ingest_batch.py` turns semi-structured WFDB headers into patient documents |
| Real-time ingestion | Kafka topic keyed by patient: per-patient ordering and a durable buffer between producers and consumers |
| Integration | The consumer embeds age/sex from `patients` into every window (join at write time, not at read time) |
| Cleaning | Causal 0.5–40 Hz Butterworth band-pass, with filter state carried across messages |
| Reduction | 2 leads → 1; 250 samples → 1 heart-rate value |
| Transformation | HR → `brady` (< 60) / `normal` / `tachy` (> 100); an alert needs 5 s of a stable abnormal class |
| **Veracity** | Detected beats are matched to the reference annotations; Se / PPV are printed and stored in `patients.quality` |
| Storage | MongoDB collections `patients`, `ecg_windows` (unique index on `(patient_id, t)`), `alerts` |
| Analytics | MongoDB aggregation pipeline: HR trend in 10-second buckets |
| Visualization | `dashboard.py`: pipeline status, patient tiles, live ECG trace, HR trend |

**Delivery guarantee.** The consumer commits Kafka offsets only *after* the MongoDB write (at-least-once). A re-delivered
message upserts its own `(patient_id, t)` document, so the stored result is exactly-once. The end-to-end test verifies this by
replaying the whole topic.

## Things to try

- **Velocity.** Restart the producer with `--speed 20`: the ingest rate grows 20× and the pipeline keeps up.
- **Fault tolerance.** Stop the consumer while the producer keeps running, then restart it. Kafka retains the messages and the
  consumer resumes from its last committed offset with no gaps. After a hard kill (`kill -9`), Kafka waits for the 10-second
  session timeout before reassigning the partitions.
- **Value.** Most of these patients take verapamil or diltiazem, drugs that slow the heart, and the stream flags their sustained
  bradycardia.

## Configuration

All settings live in `ecgpipe/config.py` and can be overridden with environment variables:

| Variable | Default |
|---|---|
| `ECG_RECORDS` | `e0103,e0104,e0105,e0106,e0107` |
| `KAFKA_BOOTSTRAP` / `KAFKA_TOPIC` / `KAFKA_PARTITIONS` / `KAFKA_GROUP` | `localhost:9092` / `ecg.raw` / `3` / `ecg-processor` |
| `MONGO_URI` / `MONGO_DB` | `mongodb://localhost:27017` / `ecg` |

## Tests

```bash
pytest   # unit tests (synthetic ECG, header parsing) + end-to-end test (runs when the services are up)
```

## Project layout

```
ecgpipe/config.py        settings (overridable via environment variables)
ecgpipe/dataset.py       PhysioNet access: download, header → patient document, signal, annotations
ecgpipe/processing.py    filtering, beat detection, heart rate, discretization, veracity matching
ecgpipe/storage.py       MongoDB collections and indexes
ecgpipe/ingest_batch.py  batch ingestion
ecgpipe/producer.py      real-time ingestion (simulated devices)
ecgpipe/consumer.py      stream processing and storage
dashboard.py             Streamlit dashboard
scripts/                 setup.sh, services.sh, demo.sh
tests/                   unit and end-to-end tests
docs/presentation.md     12-minute presentation script
```

## Data

Taddei A. et al., *The European ST-T Database: standard for evaluating systems for the analysis of ST-T changes in ambulatory
electrocardiography*, Eur Heart J 13:1164–1172 (1992). Distributed via PhysioNet (Goldberger et al., Circulation 101(23), 2000)
under the Open Data Commons Attribution License v1.0. Records are downloaded at run time and are not stored in this repository.
