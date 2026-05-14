# `data_source.py` — ARP Data Source Abstraction

## Overview

`data_source.py` defines `ARPDataSource`, a class that abstracts all data ingestion paths for the ARP anomaly detection pipeline. It supports three read modes: **batch CSV**, **streaming CSV**, and a **future Kafka stream**. All methods return a raw `DataFrame` with the schema defined in `schema.py`.

---

## Class: `ARPDataSource`

```python
class ARPDataSource:
    def __init__(self, spark: SparkSession, config: ARPConfig):
```

### Constructor Parameters

| Parameter | Type | Description |
|---|---|---|
| `spark` | `SparkSession` | Active Spark session |
| `config` | `ARPConfig` | Configuration instance providing paths and settings |

---

## Methods

### `read_csv_batch`

```python
def read_csv_batch(self, csv_dir: Optional[str] = None) -> DataFrame:
```

Reads all CSV files in a directory in **batch mode**. Used by `batch_runner.py` for historical analysis.

**Options applied:**
- `header: true` — first row treated as column names
- `mode: PERMISSIVE` — malformed rows logged, not dropped outright
- `columnNameOfCorruptRecord: _corrupt_record` — captures unparseable rows in a dedicated column
- Schema enforced via `get_arp_schema()`

**Raises:** `FileNotFoundError` is not explicitly raised here (unlike the streaming variant) — Spark will surface a read error if the path is invalid.

---

### `read_csv_stream`

```python
def read_csv_stream(self, csv_dir: Optional[str] = None) -> DataFrame:
```

Reads CSV files from a directory in **micro-batch streaming mode**. Used by `stream_runner.py`.

**Additional options vs. batch:**

| Option | Value | Description |
|---|---|---|
| `maxFilesPerTrigger` | `config.MAX_FILES_PER_TRIGGER` | Limits files consumed per micro-batch (default: 1) |
| `pathGlobFilter` | `*.csv` | Only matches CSV files |
| `recursiveFileLookup` | `false` | Does not descend into subdirectories |
| `ignoreLeadingWhiteSpace` | `true` | Trims leading whitespace from values |
| `ignoreTrailingWhiteSpace` | `true` | Trims trailing whitespace from values |

**Raises:** `FileNotFoundError` if the resolved absolute path does not exist.

> The path is converted to an absolute `file://` URI before being passed to Spark, which is required for local filesystem streaming sources.

---

### `read_kafka_stream` *(Future)*

```python
def read_kafka_stream(
    self,
    topic: Optional[str] = None,
    bootstrap_servers: Optional[str] = None
) -> DataFrame:
```

Planned real-time ingestion path via Kafka. Reads from the configured topic starting at the latest offset, deserializes the `value` column as JSON using `get_arp_schema()`, and returns a flat DataFrame.

**Not currently used in production.** Requires the `spark-sql-kafka` connector JAR on the classpath.

---

## Usage

```python
from data_source import ARPDataSource

source = ARPDataSource(spark, config)

# Batch read
raw_df = source.read_csv_batch("./Captures/CSV")

# Streaming read
raw_stream = source.read_csv_stream("./Captures/CSV")
```

---

## Design Notes

- All three methods return a DataFrame with the same schema (`get_arp_schema()`), making them interchangeable from the perspective of downstream enrichment and detection logic.
- The `csv_dir` parameter in each method takes precedence over `config.CSV_INPUT_DIR`, allowing per-call overrides without mutating the config object.
- Permissive mode is intentional: real packet captures often contain incomplete or malformed rows, and dropping them silently is preferable to a pipeline failure.