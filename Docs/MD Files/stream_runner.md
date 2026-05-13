# `stream_runner.py` — Streaming Analysis Runner

## Overview

`stream_runner.py` orchestrates the ARP anomaly detection pipeline in **streaming mode**. It sets up a continuous micro-batch processing loop, wiring each detector to a console output sink and running all queries concurrently. This mode supports both CSV file streaming and a future Kafka ingestion path.

---

## Function: `run_streaming_analysis`

```python
def run_streaming_analysis(
    spark: SparkSession,
    config: ARPConfig,
    csv_dir: Optional[str] = None,
    use_kafka: bool = False,
) -> List[StreamingQuery]:
```

### Parameters

| Parameter | Type | Description |
|---|---|---|
| `spark` | `SparkSession` | Active Spark session |
| `config` | `ARPConfig` | Configuration instance |
| `csv_dir` | `Optional[str]` | CSV input directory; falls back to `config.CSV_INPUT_DIR` |
| `use_kafka` | `bool` | If `True`, reads from Kafka instead of CSV files |

### Returns

A list of active `StreamingQuery` objects, one per detector sink.

---

## Execution Flow

```
1. Clean checkpoint directory
2. Initialize data source (CSV stream or Kafka stream)
3. Enrich raw stream → enriched stream DataFrame
4. Wire each detector to a console sink
5. Start all streaming queries concurrently
6. Await any query termination (blocking)
7. On Ctrl+C: stop all queries gracefully
```

### Checkpoint Cleanup

Identical to batch mode — the checkpoint directory is wiped at startup to ensure clean state:

```python
if os.path.exists(config.CHECKPOINT_BASE):
    shutil.rmtree(config.CHECKPOINT_BASE)
```

### Data Source Selection

```python
if use_kafka:
    raw_stream = source.read_kafka_stream()
else:
    raw_stream = source.read_csv_stream(csv_dir)
```

When `use_kafka=True`, the Kafka path is used (future capability). Default is CSV file streaming.

---

## Streaming Queries Started

| Query Name | Detector | Output Mode |
|---|---|---|
| `ARP-Scanner` | `detect_arp_scanning` | `update` |
| `GARP-Activity` | `detect_garp_activity` | `update` |
| `Spoof-ReplyMismatch` | `detect_arp_spoofing` (reply mismatch) | `append` |
| `Spoof-MACFlip` | `detect_arp_spoofing` (MAC flipping) | `update` |
| `Spoof-IPFlip` | `detect_arp_spoofing` (IP flipping) | `update` |
| `ARP-Flood` | `detect_request_flood` | `update` |
| `Unsolicited-Reply` | `detect_unsolicited_replies` | `append` |
| `MAC-Impersonation` | `detect_mac_impersonation` | `update` |
| `ARP-Conflict` | `detect_arp_conflicts` | `update` |

All queries write to the console via `OutputManager.console_sink`.

### Output Mode Rationale

- **`update`** — used for windowed aggregations that accumulate state across a window before emitting. Only changed aggregate rows are printed per trigger.
- **`append`** — used for point-in-time event detectors (reply mismatch, unsolicited replies) where each detected event is a standalone row emitted once.

---

## Concurrency and Termination

All nine queries run concurrently via Spark's internal streaming scheduler:

```python
spark.streams.awaitAnyTermination()
```

This blocks until any query terminates (e.g., due to an error or exhausted input). On `KeyboardInterrupt` (Ctrl+C), all queries are stopped gracefully:

```python
for q in queries:
    q.stop()
```

---

## Usage

```python
from stream_runner import run_streaming_analysis

queries = run_streaming_analysis(spark, config, csv_dir="./Captures/CSV")
# Blocks until terminated
```

---

## Design Notes

- Unlike batch mode, streaming mode does not cache the enriched DataFrame — Spark Structured Streaming manages its own incremental state internally.
- Each detector query is independent; a failure in one query does not terminate the others (unless `awaitAnyTermination` catches it and the caller does not restart).
- For production deployments, replace `console_sink` calls with `file_sink` or a Kafka write sink, and add per-query checkpoint paths for fault tolerance.
- The Kafka path (`use_kafka=True`) requires the `spark-sql-kafka` connector on the classpath and a running Kafka broker.