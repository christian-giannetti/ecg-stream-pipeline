# Every heartbeat, in order, exactly once

12-minute talk · 11 slides · about 950 spoken words, roughly 8½ minutes at a relaxed ~110 words per minute, plus pauses and the demo.

**Before you start:** run `scripts/demo.sh` about 3 minutes before the talk, so the heart-rate trend is full by the demo. Select patient **e0107** in the dashboard.

| # | Slide | Time | Ends at |
|---|---|---|---|
| 1 | Title | 0:20 | 0:20 |
| 2 | The problem | 0:50 | 1:10 |
| 3 | The data | 0:40 | 1:50 |
| 4 | Architecture | 0:55 | 2:45 |
| 5 | Ingestion | 1:00 | 3:45 |
| 6 | Processing | 1:10 | 4:55 |
| 7 | Storage | 1:00 | 5:55 |
| 8 | No loss, no duplicates | 0:50 | 6:45 |
| 9 | Can we trust it? | 0:50 | 7:35 |
| 10 | Live demo | 1:30 | 9:05 |
| 11 | Close | 0:50 | 9:55 |

About 2 minutes of slack remains for pauses, transitions and switching to the browser.

---

## 1 · Title

**Slide**
- **Every heartbeat, in order, exactly once**
- Streaming ECG ingestion for remote cardiac monitoring
- Kafka → Python stream processor → MongoDB → live dashboard
- [Name] · Big Data in Healthcare · Prof. A. Celesti

**Script**
> A heart monitor at home is only useful if someone sees what it records, in time. This project is a streaming pipeline that turns ECG from wearable devices into alerts a clinician can trust. I'll walk through the design choices, then show it live.

## 2 · The problem

**Slide**
- Patient e0107: 52, angina, on verapamil, a drug that slows the heart
- At home, the heart rate sits around **50 bpm**
- *How does the clinician know within seconds, and trust it?*
- Requirements:
  - continuous
  - ordered per patient
  - no loss
  - trustworthy

**Script**
> Take patient e0107: fifty-two, angina, on verapamil, a drug that slows the heart. At home, the heart rate sits around fifty beats per minute. The clinician doesn't want raw ECG. They want to know, within seconds, that the rate has stayed too low. That question sets four requirements. The data never stop, so ingestion must be real time. Each patient's signal must stay in order. Nothing may be lost when a component fails. And the result must be trustworthy, because it is clinical. The data are small on purpose: the challenge is the flow, not the cluster.

## 3 · The data

**Slide**
- PhysioNet **European ST-T Database**, the database used by Carnevale, Celesti et al. (2020)
- 5 patients · 2 h each · 250 Hz · 2 leads · ~26 MB
- Two shapes of data:
  - semi-structured headers (age, sex, diagnosis, drugs)
  - a fast signal (500 samples/s per patient)
- Every beat annotated by cardiologists → **ground truth**
- *Course: variety and velocity*

**Script**
> The data come from the European ST-T Database on PhysioNet, the same one used by Carnevale, Celesti and colleagues. Five patients, two hours each, 250 hertz, two leads: about 26 megabytes. Two things make it right for this project. It mixes two shapes of data: semi-structured headers with age, diagnosis and drugs, and a fast signal of 500 samples per second per patient. That is variety and velocity. And every beat is annotated by cardiologists, so I can check my results against ground truth.

## 4 · Architecture

**Slide**
```
[batch]  patient headers ─► ingest_batch ─► MongoDB.patients ────────┐ enrich
[stream] 5 wearables ─► Kafka "ecg.raw" ─► stream processor ─► MongoDB windows · alerts ─► dashboard
```
- *Course: generation → ingestion → preprocessing → storage → analytics → visualization*

**Script**
> The architecture has two paths. The batch path runs once and loads the patient headers into MongoDB. The streaming path never stops. Five simulated wearables publish one second of ECG per message to Kafka. A Python processor cleans it, extracts the heart rate, adds the patient data, and stores the result in MongoDB. The dashboard reads only from MongoDB. Left to right, this is the life cycle of the course: generation, ingestion, preprocessing, storage, analytics, visualization. The next slides show the choices that make each step correct.

## 5 · Ingestion: order and decoupling

**Slide**
- **Batch** for what rarely changes (patients) · **real time** for what never stops (ECG)
- 1 message = 1 patient × 1 s → low latency, without 500 messages/s per patient
- Key = `patient_id` → same partition → each patient's signal in order
- Kafka buffers: processor down ≠ data lost
- *Course: batch vs real-time ingestion · Kafka*

**Script**
> Ingestion splits by how the data behave. Patients rarely change, so they go in batch. The ECG never stops, so it goes through Kafka, the real-time ingestion tool of the course. Three choices matter. The message is one second of one patient: fast enough for alerts, without one message per sample. The key is the patient ID: Kafka routes the same key to the same partition, so each patient's signal arrives in order. And Kafka is a buffer between the devices and the processing: if the processor stops, messages wait instead of being lost.

## 6 · Processing: from waves to a label

**Slide**
1. **Cleaning:** 0.5–40 Hz band-pass filter, causal, with its state carried across messages
2. **Reduction:** 2 leads → 1 · 250 samples → 1 heart rate
3. **Transformation:** beat detection → heart rate (bpm)
4. **Discretization:** bradycardia < 60 · normal · tachycardia > 100
5. **Integration:** add age and sex from the patient document

→ An alert fires only after **5 s** in the same abnormal state.
- *Course: the preprocessing steps of the life cycle*

**Script**
> Each message goes through the preprocessing steps of the life cycle. Cleaning: a band-pass filter removes drift and noise. It only looks at past samples and carries its state across messages, so the one-second chunks join without artifacts. Reduction: two leads become one, and 250 samples become one heart rate. Transformation: a light Pan-Tompkins detector finds the beats, and the heart rate comes from the median interval between them. Discretization: the number becomes a label, bradycardia, normal or tachycardia, like the course example that turns age into teen, adult and senior. Integration: age and sex are added from the batch data. An alert fires only after five seconds in the same abnormal state, so one noisy second doesn't wake anyone.

## 7 · Storage: designed for the read

**Slide**
```
{ patient_id: "e0107", t: 812, lead: "D3",
  samples: [250 values], beats: [812.4, 813.6],
  hr: 50, hr_class: "brady",
  patient: { age: 52, sex: "M" } }
```
- **Bucket pattern:** 1 document = 1 patient × 1 s
- Patient data **embedded** → no join on read
- Trend = aggregation pipeline, 10-s buckets
- Unique index `(patient_id, t)`
- *Course: "Combine objects used together" · "Do joins while write, not on read"*

**Script**
> Storage is MongoDB, and the schema is designed around how it is read. The dashboard always needs one second of one patient, so that is one document: samples, beats, heart rate and label. This is the bucket pattern: one document instead of 250. Age and sex are copied in at write time, following the course rule: do joins while write, not on read. The trend is computed inside MongoDB, by an aggregation pipeline in ten-second buckets. That is the descriptive analytics. And one detail sets up the next slide: a unique index on patient and time.

## 8 · No loss, no duplicates

**Slide**
- Kafka offset committed **only after** the MongoDB write → at-least-once
- A message delivered again → upsert on `(patient_id, t)` → same document rewritten
- ⇒ **at-least-once delivery, exactly-once result**
- Tested:
  - processor restarted → no gaps
  - whole topic replayed → no duplicates

**Script**
> The processor commits its Kafka offset only after MongoDB has stored the data. If it crashes in between, Kafka delivers the message again. That is at-least-once, so duplicates can appear. The unique index removes them: every write is an upsert on patient and time, so a repeated message rewrites the same document. At-least-once delivery, exactly-once result. I tested both sides: a restarted processor leaves no gaps, and replaying the entire topic adds no duplicates.

## 9 · Can we trust it?

**Slide**
- **Veracity:** detector vs the cardiologists' annotations (±150 ms)
  - sensitivity 98–100 %
  - positive predictive value 98–100 %
- **Speed** (one laptop):
  - device → database ≈ 150 ms
  - at 20× real time, 3,000 / 3,000 messages stored
- **Value:**
  - all 5 patients take heart-slowing drugs
  - 3 show sustained bradycardia, and it is flagged
- *Course: veracity and value*

**Script**
> Does it work? Veracity first: in healthcare it is the V that matters most. Against the cardiologists' annotations, the detector finds 98 to 100 percent of the real beats, and 98 to 100 percent of its detections are real. On speed, one second of ECG reaches the database in about 150 milliseconds, and at twenty times real time every message is stored. And the value: all five patients take drugs that slow the heart, and three of them show sustained bradycardia, which the pipeline flags.

## 10 · Live demo

**Slide**
- `scripts/demo.sh`: one command
- services → batch ingestion → processor → dashboard → 5 wearables at 2× real time

**Script** (switch to the browser and point as you go)
> Here it is, started with one command, at twice real time. At the top, the stream is live: ten messages per second, and the latency. Each card is a patient, with the current heart rate and its label. Here are the three in bradycardia. Below is the live ECG of e0107: the orange dots are beats the processor found a fraction of a second ago. And the trend, computed by MongoDB, sits below the green normal band.

## 11 · Close

**Slide**
- Every property comes from one choice:
  - **in order** ← patient key
  - **never lost** ← Kafka buffer + commit after write
  - **exactly once** ← unique index + upsert
  - **one read per view** ← bucket + embedding
  - **trustworthy** ← validated against cardiologists
- **Limits:** simulated devices, a single node
- **Scaling out:** more partitions and consumers; MongoDB sharded on `patient_id`

**Script**
> Back to e0107. The heart slows down, and within seconds the clinician sees it, from data that arrived in order, was never lost, never duplicated, and was checked against cardiologists. Each of those properties comes from one design choice: the patient key, the Kafka buffer, commit after write with a unique index, and documents shaped for the read. The limits are clear: the devices are simulated, and everything runs on one node. Scaling out keeps the design: more partitions and consumers, and MongoDB sharded on the patient ID. Thank you.

---

## Backup answers

- **Why not write straight to MongoDB?** You would lose the buffer, and with it replay and ordering per patient. A database outage would lose data at the devices.
- **Why not a relational database?** The headers are semi-structured, and every view needs a whole window. One document per window means one read and no join.
- **Why 1-second messages?** It is the balance between latency and overhead. One message per sample would mean 500 messages/s per patient.
- **Is this really big data?** The design is what counts. Load grows linearly: 1,000 patients would be 500,000 samples/s, handled by more partitions and consumers, with the same design.
- **What if a device restarts?** A jump in time starts a fresh filter and detector state for that patient. This is covered by a test.
- **Why the median beat interval?** One missed or extra beat does not move the median.
