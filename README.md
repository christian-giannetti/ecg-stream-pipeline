# ECG stream pipeline: a lean big-data ingestion demo

This pipeline brings in **batch** clinical records and a **real-time** ECG stream. It preprocesses the signals, stores them in a
document NoSQL database, and serves descriptive analytics on a live dashboard. It is built for the course
*Big Data in Healthcare* (Prof. A. Celesti, University of Messina). The data are the PhysioNet **European ST-T Database**, the same
database used in Carnevale, Celesti, Fazio, Villari, *"A Big Data Analytics Approach for the Development of Advanced Cardiology
Applications"*, Information 2020.

```
 [batch]  WFDB headers ──ingest_batch──► MongoDB.patients ─────────┐ enrich (join at write time)
                                                                  ▼
 [stream] producer ──► Kafka topic "ecg.raw" ──► consumer ──► MongoDB.ecg_windows / alerts ──► Streamlit dashboard
          5 simulated     key = patient_id       clean, detect beats,
          wearables,      3 partitions           HR, discretize, alert
          1 msg = 1 s ECG
```

## How it maps to the course

| Big data life cycle / stack layer | Where it happens |
|---|---|
| Data generation (machine-generated, high **velocity**) | `producer.py`: 5 patients × 250 Hz × 2 leads, replayed up to N× real time |
| **Batch ingestion** | `ingest_batch.py`: semi-structured WFDB headers become patient documents (**variety**) |
| **Real-time ingestion**, interfaces & feeds | Kafka topic, keyed by patient: per-patient ordering, a buffer between producers and consumers |
| Integration | The consumer embeds age/sex from `patients` in every window: *"do joins while write, not on read"* |
| Cleaning / smoothing | Causal 0.5–40 Hz Butterworth band-pass, with filter state carried across messages |
| Reduction | 2 leads → 1; 250 samples → 1 heart-rate value |
| Transformation / discretization | HR → `brady` (<60) / `normal` / `tachy` (>100); an alert needs 5 s of a stable abnormal class |
| **Veracity** | The detector is scored against the cardiologists' beat annotations (Se / PPV, shown in the UI) |
| Operational DB (document NoSQL) | MongoDB, bucket pattern: 1 document = 1 patient × 1 s; unique index `(patient_id, t)` |
| Analytics (descriptive) | MongoDB aggregation pipelines: HR trend in 10-s buckets, time spent in each class |
| Reporting & visualization | `dashboard.py` (Streamlit + Plotly), which reads only from MongoDB |

**Delivery guarantee.** The consumer commits Kafka offsets only *after* the MongoDB write (at-least-once). A re-delivered message
upserts its own `(patient_id, t)` document, so the stored result is exactly-once. This is tested by replaying the whole topic.

## Setup (once)

Everything is installed inside one conda env: Python libs, MongoDB 8, OpenJDK 17, and Kafka 4.3 unpacked into the env.

```bash
scripts/setup.sh
conda activate ecgpipe
```

## Demo

```bash
scripts/services.sh start                   # MongoDB :27017 + Kafka (KRaft) :9092
python -m ecgpipe.ingest_batch --reset      # batch: download 5 records (~26 MB), load patients, veracity check;
                                            #   --reset clears previously streamed data for a fresh demo
python -m ecgpipe.consumer                  # terminal 1: stream processor
streamlit run dashboard.py                  # terminal 2: http://localhost:8501
python -m ecgpipe.producer --speed 1        # terminal 3: 5 wearables in real time (1 msg/s each)
```

Things to show during the demo:
- **Velocity.** Restart the producer with `--speed 20`. The ingest rate goes up 20× and the pipeline keeps up.
- **Fault tolerance.** Stop the consumer (Ctrl+C) while the producer keeps running, then start it again. Kafka kept the messages,
  and the consumer resumes from its last committed offset with no gaps. If you kill it hard (`kill -9`), Kafka first waits for
  the 10-s session timeout before it reassigns the partitions (failure detection).
- **Value.** Most of these patients are on verapamil/diltiazem, drugs that slow the heart, and the stream flags their
  sustained bradycardia.

```bash
scripts/services.sh stop                     # data persists in $CONDA_PREFIX/var/ecgpipe (delete it to reset)
```

## Tests

```bash
pytest   # unit tests (synthetic ECG, header parsing) + end-to-end test (runs when the services are up)
```

## Layout

```
ecgpipe/config.py        settings (all overridable via env vars)
ecgpipe/dataset.py       PhysioNet access: download, header → patient doc, signal, annotations
ecgpipe/processing.py    filter, beat detection, HR, discretization, veracity matching
ecgpipe/storage.py       MongoDB collections and indexes
ecgpipe/ingest_batch.py  batch ingestion
ecgpipe/producer.py      real-time ingestion (simulated devices)
ecgpipe/consumer.py      stream processing and storage
dashboard.py             demo UI
scripts/                 setup.sh, services.sh
```

## Data

Taddei A. et al., *The European ST-T Database: standard for evaluating systems for the analysis of ST-T changes in ambulatory
electrocardiography*, Eur Heart J 13:1164-1172 (1992). Distributed via PhysioNet (Goldberger et al., Circulation 101(23), 2000)
under the Open Data Commons Attribution License v1.0. The records are downloaded at run time and are not stored in this repository.
