# `config.py` — Centralized Configuration

## Overview

`config.py` defines `ARPConfig`, a dataclass that centralizes every tunable parameter used across the ARP anomaly detection system. All detectors, runners, and data sources consume this config, so changing a threshold or path in one place affects the whole pipeline.

---

## Class: `ARPConfig`

```python
@dataclass
class ARPConfig:
    ...
```

### Time Windows

| Parameter | Default | Description |
|---|---|---|
| `SCAN_WINDOW_SECONDS` | `10` | Sliding window size for scanning detection |
| `SCAN_SLIDE_SECONDS` | `5` | Slide interval for the scanning window |
| `FLOOD_WINDOW_SECONDS` | `1` | Tumbling window for request flood detection |
| `GARP_WINDOW_SECONDS` | `60` | Window for grouping GARP bursts |
| `IMPERSONATION_WINDOW_SECONDS` | `30` | Window for MAC/IP impersonation checks |
| `CONFLICT_WINDOW_SECONDS` | `10` | Window for detecting IP claim conflicts |
| `UNSOLICITED_LOOKBACK_SECONDS` | `5` | Lookback range for matching replies to requests |

### Detection Thresholds

| Parameter | Default | Description |
|---|---|---|
| `SCAN_UNIQUE_TARGETS_THRESHOLD` | `15` | Minimum unique destination IPs to flag as a scan |
| `FLOOD_REQUESTS_PER_SEC` | `50` | Minimum ARP requests per second to flag as a flood |
| `GARP_COUNT_THRESHOLD` | `1` | Minimum GARP packets in window to trigger alert |
| `IMPERSONATION_IP_COUNT_THRESHOLD` | `2` | Minimum unique source IPs from one MAC to flag impersonation |

### Paths

| Parameter | Default | Description |
|---|---|---|
| `CHECKPOINT_BASE` | `/tmp/spark-ckpt-arp` | Base directory for Spark streaming checkpoints |
| `CSV_INPUT_DIR` | `./Captures/CSV` | Default input directory for CSV packet captures |
| `OUTPUT_BASE` | `/tmp/arp_output` | Base directory for Parquet output sinks |

### Streaming

| Parameter | Default | Description |
|---|---|---|
| `MAX_FILES_PER_TRIGGER` | `1` | Number of CSV files consumed per streaming micro-batch |
| `WATERMARK_SECONDS` | `15` | Watermark delay for late-arriving event handling |

### Kafka (Future)

| Parameter | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC_ARP` | `network-arp-raw` | Kafka topic for raw ARP packet data |

---

## Usage

```python
from config import ARPConfig

config = ARPConfig()
# Override at runtime
config.CSV_INPUT_DIR = "./my-captures"
config.SCAN_UNIQUE_TARGETS_THRESHOLD = 20
```

---

## Design Notes

- Uses Python's `@dataclass` for zero-boilerplate instantiation with sensible defaults.
- All fields are mutable, so `main.py` can override them after parsing CLI arguments.
- The Kafka fields are placeholders for a planned real-time ingestion path and are not used in the current implementation.