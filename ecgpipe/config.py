"""Central settings. Every value can be overridden with an environment variable."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Data source: PhysioNet European ST-T Database (ODC-By 1.0)
PHYSIONET_DB = "edb"
RECORDS = os.getenv("ECG_RECORDS", "e0103,e0104,e0105,e0106,e0107").split(",")
DATA_DIR = Path(os.getenv("ECG_DATA_DIR", ROOT / "data" / "edb"))

# Infrastructure
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecg.raw")
KAFKA_PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "3"))
KAFKA_GROUP = os.getenv("KAFKA_GROUP", "ecg-processor")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "ecg")

# Stream / processing
WINDOW_S = 1           # one Kafka message = one bucket document = 1 s of signal
CONTEXT_S = 8          # rolling context the consumer keeps per patient for beat detection
BRADY_BPM = 60         # discretization thresholds (deck 01, "Data Transformation")
TACHY_BPM = 100
