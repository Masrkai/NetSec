# `session.py` — Spark Session Factory

## Overview

`session.py` provides a single factory function, `create_spark_session`, that constructs and returns an optimized `SparkSession` for ARP anomaly detection workloads. It handles both batch and streaming configurations and ensures consistent settings across all entry points.

---

## Function: `create_spark_session`

```python
def create_spark_session(
    app_name: str = "ARP-Anomaly-Detection",
    streaming: bool = True
) -> SparkSession:
```

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `app_name` | `str` | `"ARP-Anomaly-Detection"` | Spark UI application name |
| `streaming` | `bool` | `True` | If `True`, applies streaming-specific configuration |

### Returns

A fully configured `SparkSession` ready for use.

---

## Configuration Applied

### Always Applied

| Config Key | Value | Rationale |
|---|---|---|
| `spark.sql.shuffle.partitions` | `4` | Reduces overhead for small local datasets (default is 200) |
| `spark.sql.adaptive.enabled` | `true` | Enables Adaptive Query Execution for dynamic optimization |
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | Merges small shuffle partitions automatically |
| `spark.serializer` | `KryoSerializer` | Faster and more compact than Java default serializer |
| `spark.sql.streaming.metricsEnabled` | `true` | Exposes streaming query metrics |
| `spark.hadoop.fs.defaultFS` | `file:///` | Forces local filesystem; avoids HDFS resolution on standalone runs |

### Streaming-Only

| Config Key | Value | Rationale |
|---|---|---|
| `spark.sql.streaming.forceDeleteTempCheckpointLocation` | `true` | Cleans up temporary checkpoint dirs on restart |
| `spark.sql.streaming.checkpointLocation` | `{CHECKPOINT_BASE}/global` | Global fallback checkpoint path for streaming queries |

### Log Level

Set to `WARN` to suppress INFO-level Spark noise while keeping actionable warnings and errors visible.

---

## Usage

```python
from session import create_spark_session

# For batch mode
spark = create_spark_session(streaming=False)

# For streaming mode (default)
spark = create_spark_session()
```

---

## Design Notes

- The streaming checkpoint path is derived from `ARPConfig.CHECKPOINT_BASE` at module import time. If you change `CHECKPOINT_BASE` on a config instance at runtime, this value will not reflect that change — the global default is baked in at session creation.
- `spark.hadoop.fs.defaultFS = file:///` is critical for local-filesystem CSV reads; without it, Spark may attempt to resolve paths against a nonexistent HDFS cluster.
- Adaptive Query Execution (`AQE`) is safe to leave on for both batch and streaming; it provides automatic plan optimizations with no downside for this use case.