# `output.py` — Output Sink Manager

## Overview

`output.py` defines `OutputManager`, a helper class that wraps Spark Structured Streaming write operations into reusable sink methods. It is used exclusively in streaming mode (`stream_runner.py`) to route detector output to the console, Parquet files, or in-memory tables.

---

## Class: `OutputManager`

```python
class OutputManager:
    def __init__(self, spark: SparkSession, config: ARPConfig):
```

### Constructor Parameters

| Parameter | Type | Description |
|---|---|---|
| `spark` | `SparkSession` | Active Spark session |
| `config` | `ARPConfig` | Config instance providing `OUTPUT_BASE` and `CHECKPOINT_BASE` paths |

---

## Methods

### `console_sink`

```python
def console_sink(
    self,
    df: DataFrame,
    query_name: str,
    output_mode: str = "update"
) -> StreamingQuery:
```

Writes streaming results to stdout. Used for development and real-time monitoring.

| Option | Value |
|---|---|
| `truncate` | `false` — full column values printed |
| `numRows` | `20` — up to 20 rows per micro-batch |

**Output mode:** `"update"` by default (only changed rows since last trigger). Use `"append"` for detectors that produce point-in-time events (e.g., unsolicited replies, reply mismatches).

---

### `file_sink`

```python
def file_sink(
    self,
    df: DataFrame,
    query_name: str,
    subdir: str,
    output_mode: str = "append"
) -> StreamingQuery:
```

Writes streaming results to **Parquet files** under `{OUTPUT_BASE}/{subdir}`. Suitable for downstream batch analysis, dashboarding, or archival.

- Output path is created automatically (`os.makedirs`).
- Checkpoint location: `{CHECKPOINT_BASE}/{subdir}` — ensures exactly-once delivery and query resumability.
- Default output mode is `"append"`, which is required for file sinks.

---

### `memory_sink`

```python
def memory_sink(
    self,
    df: DataFrame,
    query_name: str
) -> StreamingQuery:
```

Writes results to an in-memory table named `query_name`. Queryable via `spark.sql(f"SELECT * FROM {query_name}")` for interactive exploration.

- Always uses `"complete"` output mode (replaces the full table each trigger).
- Not suitable for production — memory is not persisted across restarts.

---

## Output Mode Reference

| Mode | Description | Typical Use |
|---|---|---|
| `append` | Only new rows emitted since last trigger | Point events (reply mismatch, unsolicited reply), file sinks |
| `update` | Only rows that changed since last trigger | Windowed aggregations (scanning, flood, GARP) |
| `complete` | Full result table emitted each trigger | Memory sink, small aggregations |

---

## Usage

```python
from output import OutputManager

output = OutputManager(spark, config)

# Console output for scanning detector
query = output.console_sink(scanning_df, "ARP-Scanner", output_mode="update")

# Parquet output for archival
query = output.file_sink(garp_df, "GARP-Activity", subdir="garp", output_mode="append")
```

---

## Design Notes

- `console_sink` is the default in `stream_runner.py`; `file_sink` and `memory_sink` are available for production or interactive use cases but are not wired by default.
- Each sink call starts an independent streaming query. Spark runs all queries concurrently — each processes the same enriched stream independently.
- The `query_name` is displayed in the Spark Streaming UI and in console headers, so choosing descriptive names aids monitoring.