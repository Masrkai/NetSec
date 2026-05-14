# `main.py` — Entry Point

## Overview

`main.py` is the command-line entry point for the ARP Anomaly Detection System. It parses arguments, initializes configuration and the Spark session, and delegates execution to either the batch or streaming runner.

---

## Usage

```bash
python main.py [--mode {batch,stream}] [--csv-dir PATH] [--kafka]
```

### Arguments

| Argument | Type | Default | Description |
|---|---|---|---|
| `--mode` | `choice` | `batch` | Execution mode: `batch` for historical CSV analysis, `stream` for continuous processing |
| `--csv-dir` | `str` | `./Captures/CSV` | Path to the directory containing tshark/Wireshark CSV exports |
| `--kafka` | `flag` | `False` | Enable Kafka as the streaming source instead of CSV files (streaming mode only; future capability) |

---

## Execution Flow

```
1. Parse CLI arguments
2. Instantiate ARPConfig (defaults)
3. Override config.CSV_INPUT_DIR with --csv-dir value
4. Create SparkSession (streaming=True if mode is "stream")
5. Dispatch:
   ├── mode == "batch"  → run_batch_analysis(spark, config, csv_dir)
   └── mode == "stream" → run_streaming_analysis(spark, config, csv_dir, kafka)
6. spark.stop() in finally block (always executed)
```

The `spark.stop()` call is in a `finally` block, ensuring the Spark context is released cleanly even if the runner raises an exception.

---

## Examples

**Run batch analysis on default CSV directory:**
```bash
python main.py
```

**Run batch analysis on a custom directory:**
```bash
python main.py --mode batch --csv-dir /data/captures/2024-06-01
```

**Run streaming analysis on CSV files:**
```bash
python main.py --mode stream --csv-dir ./Captures/CSV
```

**Run streaming analysis from Kafka (future):**
```bash
python main.py --mode stream --kafka
```

---

## Design Notes

- `config.CSV_INPUT_DIR` is overridden from the CLI argument after instantiation. If `--csv-dir` is not provided, `ARPConfig`'s default (`./Captures/CSV`) is used for both the config attribute and the runner argument.
- `create_spark_session` receives `streaming=(args.mode == "stream")`, which applies streaming-specific Spark configuration only when needed.
- The `--kafka` flag is wired through but requires the Kafka connector JAR and a running broker to function; it does nothing in batch mode.