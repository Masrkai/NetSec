### 1. ARP Scanning / Host Discovery

**What it looks like in your data:**  
MAC `62:5e:34:e9:d2:73` sends a rapid burst of ARP requests (`opcode=1`) for a contiguous sequence of IPs – from `192.168.1.2` up to `192.168.1.254` – in less than a second (around timestamp 1778013859). This is a textbook ARP scan; the attacker is mapping live hosts on the subnet.

**Detection with Spark Streaming:**

- Windowed count of **unique destination IPs per source MAC** over a short sliding window (e.g., 10 seconds).
- If a single MAC sends requests to more than *N* distinct IPs (e.g., 20), flag it as a scanner.

```python
scanners = arp_df.filter(col("arp.opcode") == 1) \
    .withWatermark("ts", "10 seconds") \
    .groupBy(window("ts", "10 seconds", "5 seconds"), "eth.src") \
    .agg(countDistinct("arp.dst.proto_ipv4").alias("unique_dst_ips")) \
    .filter("unique_dst_ips > 20")
```

---

### 2. Gratuitous ARP (GARP) Abuse

**What it looks like in your data:**  
Several packets have `arp.isgratuitous = True`. For example, `00:e0:4c:36:70:d5` announces its own IP `192.168.1.5` repeatedly (gratuitous ARP). While some GARP is legitimate (e.g., IP change notification), excessive or unsolicited GARP can be used for **ARP spoofing / man‑in‑the‑middle**, forcing switches to update their CAM tables incorrectly.

**Detection:**

- Monitor the rate of gratuitous ARP packets per source MAC. A spike (e.g., > 5 GARP in 10 seconds) is suspicious.
- Cross‑check with IP‑MAC history: if a GARP announces a new MAC for an existing IP, it’s an attack.

```python
garp_flood = arp_df.filter(col("arp.isgratuitous") == True) \
    .groupBy(window("ts", "10 seconds"), "eth.src") \
    .count() \
    .filter("count > 5")
```

---

### 3. ARP Spoofing (IP‑MAC Mismatch Over Time)

**What it looks like in your data:**  
The legitimate IP‑MAC pairs should be stable. A change might indicate spoofing. For instance, earlier `44:fb:5a:d1:93:49` was associated with `192.168.1.1` (seen in a reply), but if later that IP is claimed by a different MAC, it’s a red flag. Your data does show a possible anomaly: at timestamp `1778013932.379287104`, MAC `62:5e:34:e9:d2:73` sends an ARP request with `arp.src.hw_mac = 44:fb:5a:d1:93:49` and `arp.src.proto_ipv4 = 192.168.1.1` — this is an ARP **request** claiming to be from `192.168.1.1` but with a different source MAC than the real router (`44:fb:5a:d1:93:49` is the real router MAC, but here it’s sent from `62:5e:34:e9:d2:73` — wait, careful: The row is:

```
1778013932.379287104,62:5e:34:e9:d2:73,00:e0:4c:36:70:d5,1,44:fb:5a:d1:93:49,192.168.1.1,00:00:00:00:00:00,192.168.1.9,
```

So the *source MAC* at Ethernet layer is `62:5e:34:e9:d2:73`, but the ARP sender hardware address is `44:fb:5a:d1:93:49` (the router’s MAC). This is an attempted ARP spoof: the attacker is crafting an ARP request that appears to come from the router (`192.168.1.1`) to poison the victim’s cache, perhaps pretending to be the default gateway. This is a clear spoofing attempt.

**Detection:**

- Maintain a stateful table of `IP -> MAC` mappings (built from ARP replies and unsolicited GARP). Whenever an ARP packet arrives, check:
  - For an **ARP reply**: if the `src_mac` for a known IP differs from the stored MAC, alert.
  - For an **ARP request**: if the `arp.src.hw_mac` does not match the `eth.src` (i.e., the sender hardware address is spoofed), alert.
- In Spark Structured Streaming, use `flatMapGroupsWithState` to keep per‑IP state and detect changes.

---

### 4. ARP Request Flooding (DoS)

**What it looks like in your data:**  
MAC `62:5e:34:e9:d2:73` sends hundreds of ARP requests per second. That’s a high rate, potentially overwhelming network devices.

**Detection:**

- Simple rate limit check: count requests per source MAC per second, alert if > threshold (e.g., 100 req/s).

```python
flood = arp_df.filter(col("arp.opcode") == 1) \
    .groupBy(window("ts", "1 second"), "eth.src") \
    .count() \
    .filter("count > 100")
```

---

### 5. Unsolicited ARP Reply (Reply Without Prior Request)

**What it looks like in your data:**  
A normal ARP reply should follow a request. An attacker might send a crafted reply (gratuitous or not) without any matching request to inject false mappings. For example, the replies from `00:e0:4c:36:70:d5` at timestamps `1778013863.077513895` and `1778013866.633834817` are replies to the scans, so they are legitimate responses. However, an unsolicited reply to the broadcast address or directly to a victim would be suspicious.

**Detection:**

- Correlate requests and replies via stream‑stream join (using `dst_ip = src_ip` of reply and matching request within a short time window). Replies that can’t be matched to a prior request within a timeout are potential attacks.

---

### 6. MAC Address Impersonation (Duplicate MAC Usage)

**What it looks like:**  
The MAC `44:fb:5a:d1:93:49` appears as both `eth.src` and inside ARP headers as `arp.src.hw_mac` in some requests, but we also see the attacker’s MAC (`62:5e:34:e9:d2:73`) using that MAC in the spoofed request. While not the same layer, a monitoring system could detect that a MAC is being claimed by different IPs in a short span.

**Detection:**

- Track the set of IPs associated with each MAC and vice versa. If a MAC is seen with many different IPs, it could be a sign of MAC flooding or impersonation.

---

### Implementation with Spark Structured Streaming (Example Snippet for Spoofing Detection)

```python
from pyspark.sql.functions import col, window, countDistinct, concat, lit
from pyspark.sql.types import *

# Assume arp_parsed DF with columns: ts (timestamp), eth.src, arp.opcode, arp.src.hw_mac, arp.src.proto_ipv4, arp.dst.proto_ipv4, arp.isgratuitous

# Windowed scanner detection
scanner_alerts = arp_parsed.filter("arp.opcode == 1") \
    .withWatermark("ts", "10 seconds") \
    .groupBy(window("ts", "10 seconds"), "eth.src") \
    .agg(countDistinct("arp.dst.proto_ipv4").alias("targets")) \
    .filter("targets > 20") \
    .selectExpr("window.start as start_time", "eth.src as scanner_mac", "targets")

# Spoof detection: ARP request with mismatched src MAC
spoofed_requests = arp_parsed.filter(
    (col("arp.opcode") == 1) &
    (col("eth.src") != col("arp.src.hw_mac"))
).select("ts", "eth.src", "arp.src.hw_mac", "arp.src.proto_ipv4", "arp.dst.proto_ipv4")

# Write alerts to console or Kafka
scanner_alerts.writeStream.outputMode("update").format("console").start()
spoofed_requests.writeStream.outputMode("append").format("console").start()
```

---

### 7. Beyond Rule‑Based: Unsupervised Machine Learning

Once you accumulate historical data, you can train an **Isolation Forest** or **autoencoder** on features like request rate, unique IPs contacted, reply/request ratio, and GARP frequency. A streaming ML pipeline (Spark MLlib + Structured Streaming) can then score each source MAC and flag outliers. This catches novel attack patterns that rule‑based systems might miss.

---

### Summary of Detectable Anomalies and Their Signals

| Anomaly                  | Key Indicators in Data                            | Frameworks/Techniques                     |
|--------------------------|---------------------------------------------------|-------------------------------------------|
| ARP Scanning             | High distinct dst IPs per src MAC in a short window | Windowed aggregation, approx count distinct |
| ARP Spoofing             | `eth.src` ≠ `arp.src.hw_mac` in request; IP‑MAC remapping | Stateful mapping, stream‑stream joins     |
| Gratuitous ARP Storm     | High rate of `arp.isgratuitous` = True             | Rate monitoring                           |
| ARP Request Flood (DoS)  | Extreme request rate from a single MAC             | Tumbling/ sliding window count            |
| Unsolicited Reply        | Reply without prior request within time window     | Stream‑stream left outer join             |
| MAC Flooding             | One MAC associated with many IPs in short time     | Distinct count of IPs per MAC             |

By running such detectors on Spark Streaming (or Flink), you can consume the same CSV‑like data from a live capture (piped from tshark into Kafka) and generate real‑time alerts for all these attack types. The enriched dataset you’ve prepared is already ideal to feed into those analytical pipelines.
