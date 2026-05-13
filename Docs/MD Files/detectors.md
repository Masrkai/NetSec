# `detectors.py` — ARP Anomaly Detectors

## Overview

`detectors.py` implements all seven stateless anomaly detectors. Each function accepts an enriched ARP DataFrame (from `enrichment.py`) and an `ARPConfig` instance, and returns one or more DataFrames describing detected anomaly events. All detectors operate purely within their configured time windows — there is no cross-batch state or learned baseline.

---

## Detector 1: `detect_arp_scanning`

```python
def detect_arp_scanning(df: DataFrame, config: ARPConfig) -> DataFrame:
```

Detects hosts sending ARP requests to an unusually large number of distinct IP addresses in a short window — a hallmark of network scanning.

**Filter:** `is_request AND is_broadcast`

**Window:** Sliding — `SCAN_WINDOW_SECONDS` size, `SCAN_SLIDE_SECONDS` slide

**Threshold:** `unique_targets >= SCAN_UNIQUE_TARGETS_THRESHOLD`

**Output Columns:**

| Column | Description |
|---|---|
| `window_start` / `window_end` | Detection window bounds |
| `scanner_mac` | Ethernet source MAC of the scanner |
| `unique_targets` | Number of distinct destination IPs queried |
| `total_requests` | Total ARP requests sent in the window |
| `targeted_ip_count` | Size of the collected target IP set |
| `window_first_ts` / `window_last_ts` | First and last packet timestamps |

---

## Detector 2: `detect_garp_activity`

```python
def detect_garp_activity(df: DataFrame, config: ARPConfig) -> DataFrame:
```

Flags hosts sending Gratuitous ARP (GARP) packets. GARPs are legitimate during network changes but are also a primary tool for ARP poisoning attacks.

**Filter:** `is_gratuitous == True`

**Window:** Tumbling — `GARP_WINDOW_SECONDS`

**Threshold:** `garp_count >= GARP_COUNT_THRESHOLD`

**Output Columns:**

| Column | Description |
|---|---|
| `window_start` / `window_end` | Detection window bounds |
| `mac` | Source MAC address |
| `garp_count` | Total GARP packets in window |
| `claimed_ip_count` | Number of distinct IPs claimed |
| `first_garp` / `last_garp` | Earliest and latest GARP timestamps |

---

## Detector 3: `detect_arp_spoofing`

```python
def detect_arp_spoofing(df: DataFrame, config: ARPConfig) -> Tuple[DataFrame, DataFrame, DataFrame]:
```

Returns **three DataFrames**, each targeting a distinct spoofing heuristic.

### Heuristic A — Reply MAC Mismatch (`reply_mismatch`)

Flags ARP reply packets where the Ethernet source MAC (`eth_src`) differs from the sender MAC in the ARP payload (`arp_src_mac`). This L2/L3 mismatch is a direct indicator of crafted spoofed replies.

**Filter:** `is_reply AND has_mac_mismatch`

**Output Columns:** `event_ts`, `real_mac`, `claimed_mac`, `claimed_ip`, `target_ip`, `spoof_heuristic`

---

### Heuristic B — MAC Flipping (`mac_flipping`)

Detects a single Ethernet source (`eth_src`) advertising multiple different ARP sender MACs within a time window — suggesting MAC identity cycling.

**Filter:** `(is_reply OR is_gratuitous) AND arp_src_mac IS NOT NULL AND arp_src_mac != "00:00:00:00:00:00"`

**Threshold:** `unique_arp_macs > 1`

**Output Columns:** `window_start`, `window_end`, `eth_mac`, `unique_arp_macs`, `unique_arp_ips`, `identity_count`, `packet_count`, `spoof_heuristic`

---

### Heuristic C — IP Flipping (`ip_flipping`)

Detects a single Ethernet source claiming more than `IMPERSONATION_IP_COUNT_THRESHOLD` distinct source IPs in a window — a sign of rapid IP identity changes.

**Filter:** `(is_reply OR is_gratuitous OR is_request) AND arp_src_ip IS NOT NULL AND arp_src_ip != "0.0.0.0"`

**Threshold:** `unique_src_ips >= IMPERSONATION_IP_COUNT_THRESHOLD`

**Output Columns:** `window_start`, `window_end`, `mac`, `unique_src_ips`, `ip_count`, `packet_count`, `spoof_heuristic`

---

## Detector 4: `detect_request_flood`

```python
def detect_request_flood(df: DataFrame, config: ARPConfig) -> DataFrame:
```

Flags hosts sending an excessive number of ARP requests within a one-second window, indicative of DoS or aggressive scanning behavior.

**Filter:** `is_request`

**Window:** Tumbling — `FLOOD_WINDOW_SECONDS` (typically 1 second)

**Threshold:** `requests_per_sec >= FLOOD_REQUESTS_PER_SEC`

**Output Columns:**

| Column | Description |
|---|---|
| `second` | Window start time |
| `mac` | Source MAC of the flooding host |
| `requests_per_sec` | Total requests in the 1-second window |
| `unique_targets` | Distinct destination IPs targeted |
| `second_start` / `second_end` | First and last packet timestamps |

---

## Detector 5: `detect_unsolicited_replies`

```python
def detect_unsolicited_replies(df: DataFrame, config: ARPConfig) -> DataFrame:
```

Detects ARP reply packets that have no corresponding ARP request within the lookback window. Unsolicited replies are a canonical ARP poisoning technique.

**Approach:** Left join of replies onto requests, matching on `rep_src_ip = req_dst_ip AND rep_dst_ip = req_src_ip` within the time interval `[req_ts, req_ts + UNSOLICITED_LOOKBACK_SECONDS]`. Rows where `req_ts IS NULL` after the join are unsolicited.

**Output Columns:**

| Column | Description |
|---|---|
| `time` | Timestamp of the unsolicited reply |
| `mac` | Source MAC of the sender |
| `ip` | IP address being advertised |
| `advertised_mac` | MAC address being advertised |
| `alert` | `"Unsolicited ARP reply"` |
| `reason` | `"No matching request in lookback window"` |

> **Note:** This detector uses a time-bounded left join and is the most computationally expensive detector. In streaming mode, it requires the watermark to be set correctly to avoid unbounded state accumulation.

---

## Detector 6: `detect_mac_impersonation`

```python
def detect_mac_impersonation(df: DataFrame, config: ARPConfig) -> DataFrame:
```

Flags a single Ethernet source that either claims multiple source IPs or advertises multiple distinct ARP sender MACs within a window — a broader impersonation signal that combines elements of the spoofing heuristics.

**Filter:** `is_request OR is_reply`

**Window:** Tumbling — `IMPERSONATION_WINDOW_SECONDS`

**Threshold:** `unique_src_ips >= IMPERSONATION_IP_COUNT_THRESHOLD OR unique_arp_macs > 1`

**Output Columns:**

| Column | Description |
|---|---|
| `window_start` / `window_end` | Detection window bounds |
| `mac` | Source Ethernet MAC |
| `unique_src_ips` | Distinct source IPs claimed |
| `unique_arp_macs` | Distinct ARP sender MACs used |
| `identity_count` | Size of `(ip, mac)` identity pairs observed |
| `total_packets` | Total packets from this MAC in window |
| `alert_type` | `"MAC impersonation"` |

---

## Detector 7: `detect_arp_conflicts`

```python
def detect_arp_conflicts(df: DataFrame, config: ARPConfig) -> DataFrame:
```

Detects IP address conflicts — cases where multiple distinct MAC addresses claim ownership of the same IP within a short window. This can indicate duplicate IP assignment or an ongoing ARP spoofing attack.

**Filter:** `(is_reply OR is_gratuitous) AND arp_src_ip IS NOT NULL AND arp_src_ip != "0.0.0.0"`

**Window:** Tumbling — `CONFLICT_WINDOW_SECONDS`

**Threshold:** `claiming_mac_count > 1` (more than one distinct MAC claiming the same IP)

**Output Columns:**

| Column | Description |
|---|---|
| `window_start` / `window_end` | Detection window bounds |
| `contested_ip` | IP address being claimed by multiple MACs |
| `claiming_mac_count` | Number of distinct MACs claiming the IP |
| `mac_count` | Same as `claiming_mac_count` (size of set) |
| `claim_count` | Total packets contributing to the conflict |
| `conflict_type` | `"IP conflict"` |

---

## Detector Summary

| # | Function | Attack / Anomaly | Key Threshold |
|---|---|---|---|
| 1 | `detect_arp_scanning` | ARP network scanning | `SCAN_UNIQUE_TARGETS_THRESHOLD` |
| 2 | `detect_garp_activity` | Gratuitous ARP abuse | `GARP_COUNT_THRESHOLD` |
| 3a | `detect_arp_spoofing` (reply mismatch) | Crafted spoofed replies | `has_mac_mismatch` |
| 3b | `detect_arp_spoofing` (MAC flipping) | MAC identity cycling | `unique_arp_macs > 1` |
| 3c | `detect_arp_spoofing` (IP flipping) | Rapid IP claiming | `IMPERSONATION_IP_COUNT_THRESHOLD` |
| 4 | `detect_request_flood` | ARP DoS / flood | `FLOOD_REQUESTS_PER_SEC` |
| 5 | `detect_unsolicited_replies` | Poisoning via unsolicited replies | Lookback join miss |
| 6 | `detect_mac_impersonation` | Broad identity spoofing | `IMPERSONATION_IP_COUNT_THRESHOLD` |
| 7 | `detect_arp_conflicts` | IP conflict / duplicate assignment | `claiming_mac_count > 1` |