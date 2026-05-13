# `enrichment.py` — ARP Data Enrichment

## Overview

`enrichment.py` provides the `enrich_arp_data` function, which transforms raw ingested ARP packet data into a clean, analytics-ready DataFrame. It normalizes string fields, casts types, derives boolean feature flags, and registers a watermark for streaming windowed operations.

---

## Function: `enrich_arp_data`

```python
def enrich_arp_data(raw_df: DataFrame, config: ARPConfig) -> DataFrame:
```

### Parameters

| Parameter | Type | Description |
|---|---|---|
| `raw_df` | `DataFrame` | Raw ARP DataFrame from `ARPDataSource` |
| `config` | `ARPConfig` | Config instance, used for `WATERMARK_SECONDS` |

### Returns

A cleaned and enriched `DataFrame` with the columns listed below.

---

## Processing Steps

### 1. Column Renaming and Type Casting

Raw tshark column names (dot-notation) are aliased to clean snake_case equivalents. Types are cast where needed:

| Raw Column | Output Column | Transformation |
|---|---|---|
| `frame.time_epoch` | `epoch_ts` | Cast to `double` |
| `eth.src` | `eth_src_raw` | String alias |
| `eth.dst` | `eth_dst_raw` | String alias |
| `arp.opcode` | `opcode` | Cast to `int` |
| `arp.src.hw_mac` | `arp_src_mac_raw` | String alias |
| `arp.src.proto_ipv4` | `arp_src_ip_raw` | String alias |
| `arp.dst.hw_mac` | `arp_dst_mac_raw` | String alias |
| `arp.dst.proto_ipv4` | `arp_dst_ip_raw` | String alias |
| `arp.isgratuitous` | `gratuitous_raw` | String alias |

### 2. Timestamp Derivation

```python
event_ts = from_unixtime(epoch_ts).cast("timestamp")
```

Converts the Unix epoch float into a proper Spark `TimestampType`, required for windowed aggregations.

### 3. String Normalization

All MAC and IP fields are lowercased and trimmed to eliminate case inconsistencies and whitespace noise from CSV exports:

```python
eth_src = lower(trim(eth_src_raw))
arp_src_mac = lower(trim(arp_src_mac_raw))
arp_src_ip = trim(arp_src_ip_raw)
# ... etc.
```

### 4. Boolean Feature Flags

| Column | Logic | Description |
|---|---|---|
| `is_gratuitous` | `lower(trim(gratuitous_raw)).isin(["true", "1", "yes", "t", "y"])` | Normalizes tshark's multi-format boolean |
| `is_broadcast` | `eth_dst == "ff:ff:ff:ff:ff:ff"` | Ethernet broadcast destination |
| `is_request` | `opcode == 1` | ARP request |
| `is_reply` | `opcode == 2` | ARP reply |
| `has_mac_mismatch` | `eth_src != arp_src_mac AND arp_src_mac IS NOT NULL AND arp_src_mac != "00:00:00:00:00:00"` | Layer 2 / Layer 3 MAC address mismatch, a spoofing indicator |

### 5. Watermark Registration

```python
.withWatermark("event_ts", f"{config.WATERMARK_SECONDS} seconds")
```

Registers a watermark on `event_ts` to enable stateful streaming operations (windowed joins and aggregations) to drop late-arriving data safely.

---

## Output Schema

| Column | Type | Description |
|---|---|---|
| `event_ts` | `Timestamp` | Parsed packet timestamp |
| `epoch_ts` | `Double` | Original Unix epoch timestamp |
| `eth_src` | `String` | Normalized source MAC (Ethernet layer) |
| `eth_dst` | `String` | Normalized destination MAC (Ethernet layer) |
| `opcode` | `Integer` | ARP opcode (`1` or `2`) |
| `arp_src_mac` | `String` | Normalized sender MAC (ARP payload) |
| `arp_src_ip` | `String` | Trimmed sender IP (ARP payload) |
| `arp_dst_mac` | `String` | Normalized target MAC (ARP payload) |
| `arp_dst_ip` | `String` | Trimmed target IP (ARP payload) |
| `is_gratuitous` | `Boolean` | Gratuitous ARP flag |
| `is_broadcast` | `Boolean` | Ethernet broadcast flag |
| `is_request` | `Boolean` | ARP request flag |
| `is_reply` | `Boolean` | ARP reply flag |
| `has_mac_mismatch` | `Boolean` | L2/L3 MAC mismatch flag |

---

## Usage

```python
from enrichment import enrich_arp_data

enriched = enrich_arp_data(raw_df, config)
enriched.cache()  # Cache in batch mode for multi-detector reuse
```

---

## Design Notes

- The enrichment layer is the **single normalization point** in the pipeline — all detectors consume the enriched DataFrame and rely on its boolean flags rather than re-implementing opcode checks or MAC comparisons.
- `is_gratuitous` deliberately handles multiple string representations because tshark's output for this field varies across versions and platforms.
- `has_mac_mismatch` excludes the all-zeros MAC (`00:00:00:00:00:00`) to avoid false positives from probing packets with unresolved target MACs.
- The watermark is applied here (not in individual detectors) so it is registered once, consistently, regardless of which detectors run.