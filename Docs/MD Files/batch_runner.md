# `batch_runner.py` — Batch Analysis Runner

## Overview

`batch_runner.py` orchestrates the full ARP anomaly detection pipeline in **batch mode**. It reads historical CSV data, enriches it, runs all seven detectors, prints per-detector results, and returns a dictionary of result DataFrames. This mode is stateless — no checkpoints carry over between runs.

---

## Function: `run_batch_analysis`

```python
def run_batch_analysis(
    spark: SparkSession,
    config: ARPConfig,
    csv_dir: Optional[str] = None
) -> Dict[str, DataFrame]:
```

### Parameters

| Parameter | Type | Description |
|---|---|---|
| `spark` | `SparkSession` | Active Spark session |
| `config` | `ARPConfig` | Configuration instance |
| `csv_dir` | `Optional[str]` | Path to CSV input directory; falls back to `config.CSV_INPUT_DIR` |

### Returns

A dictionary mapping detection category names (strings) to result DataFrames. Keys:

| Key | Detector |
|---|---|
| `"scanning"` | ARP scanning |
| `"garp"` | GARP activity |
| `"spoof_reply_mismatch"` | Spoofing — reply MAC mismatch |
| `"spoof_mac_flipping"` | Spoofing — MAC flipping |
| `"spoof_ip_flipping"` | Spoofing — IP flipping |
| `"flood"` | Request flood |
| `"unsolicited"` | Unsolicited replies |
| `"impersonation"` | MAC impersonation |
| `"conflict"` | ARP conflict |

Returns an **empty dict** if no raw data is found.

---

## Execution Flow

```
1. Clean and recreate checkpoint directory
2. Read raw CSV data (batch mode)
3. Count and validate raw records
4. Enrich data → cache enriched DataFrame
5. Run each detector in sequence
6. Print per-detector counts and sample rows
7. Print summary statistics
8. Unpersist cached DataFrame
9. Return results dict
```

### Checkpoint Cleanup

At startup, the checkpoint base directory (`config.CHECKPOINT_BASE`) is deleted and recreated:

```python
if os.path.exists(config.CHECKPOINT_BASE):
    shutil.rmtree(config.CHECKPOINT_BASE)
os.makedirs(config.CHECKPOINT_BASE, exist_ok=True)
```

This ensures a clean slate for each batch run, preventing stale streaming state from interfering.

### Caching

The enriched DataFrame is cached with `.cache()` before any detector runs. Since each detector triggers a full scan of the enriched data, caching avoids re-reading and re-parsing the CSV files multiple times.

```python
enriched = enrich_arp_data(raw_df, config)
enriched.cache()
# ... all detectors run ...
enriched.unpersist()
```

---

## Summary Output

After all detectors run, a summary block prints:

- Total packet count
- Breakdown: requests, replies, GARPs
- Per-detector event count with status emoji

```
SUMMARY
============================
Total packets: 12543
  Requests: 8201
  Replies:  4102
  GARPs:    240

Detections:
  Scanning                 :   3 events 🔴 ALERT
  GARP                     :  12 events 🔴 ALERT
  Spoof (reply mismatch)   :   0 events 🟢 Clean
  ...
```

---

## Usage

```python
from batch_runner import run_batch_analysis

results = run_batch_analysis(spark, config, csv_dir="./Captures/CSV")
scanning_df = results.get("scanning")
```

---

## Design Notes

- Detectors are run sequentially; since each calls `.count()` and optionally `.show()`, the run time scales with the number of detectors and data volume.
- The function is intentionally verbose in its console output — it is designed for interactive investigative use, not silent pipeline execution.
- Returning the results dict allows callers to perform additional analysis, export to other formats, or compose higher-level reports.