## 1. Overall Architecture

```
[PCAP File] → [Ingestion] → [HDFS Storage] → [Spark Processing] → [Detection Output] → [Visualisation]
```

- **Storage**: Hadoop HDFS (or a local file system simulated as HDFS)
- **Compute**: Apache Spark (PySpark recommended)
- **Analysis**: Spark SQL + DataFrames for rule‑based detection, Spark MLlib for machine learning
- **Notebooks**: Jupyter with PySpark, or Apache Zeppelin for interactive exploration
- **Optional Frontend**: Elasticsearch + Kibana, or a simple Streamlit dashboard

Spark is far better suited than plain Hadoop MapReduce because you’ll need iterative data exploration, windowed operations, and possibly machine learning – all of which Spark handles efficiently.

---

## 2. Step‑by‑Step Roadmap

### Step 1 – Convert PCAP to Structured Data

Wireshark’s `.pcap` is binary. For big data processing, transform it into a columnar format like **Parquet** (splittable, compressed, schema‑aware) or at least CSV/JSON.

**Tools you can utilise**:

- **Tshark** (command‑line Wireshark)

  ```bash
  tshark -r attack.pcap -T fields -e frame.time_epoch -e eth.src -e eth.dst \
    -e arp.opcode -e arp.src.hw_mac -e arp.src.proto_ipv4 \
    -e arp.dst.hw_mac -e arp.dst.proto_ipv4 -E header=y -E separator=, > arp_traffic.csv
  ```

- **Python + Scapy / DPKT** – more flexible, allows custom feature extraction and direct writing to Parquet (e.g., using PyArrow).

The output should contain at least: `timestamp`, `src_mac`, `dst_mac`, `arp_opcode` (1=request, 2=reply), `sender_mac`, `sender_ip`, `target_mac`, `target_ip`.

### Step 2 – Ingest into HDFS and Create a Spark DataFrame

Place the CSV/Parquet file in HDFS (or a local path if you’re testing in standalone mode). Then load it with Spark:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ARP_Attack_Detection").getOrCreate()

df = spark.read.option("header", True).csv("hdfs:///data/arp_traffic.csv")
# or parquet
df = spark.read.parquet("hdfs:///data/arp_traffic.parquet")
df.printSchema()
df.createOrReplaceTempView("arp_packets")
```

### Step 3 – Feature Engineering with Spark SQL/DataFrames

Derive features that highlight ARP poisoning patterns:

- Time‑based windows: group packets into 1‑second or 5‑second windows.
- IP‑to‑MAC mappings: for each IP, track which MAC addresses claim it.
- Unusual ARP replies: gratuitous ARP (target IP = sender IP, often unsolicited), or replies without a preceding request.
- MAC flapping: a single IP mapped to multiple MACs within a short time.

Example using Spark SQL:

```sql
-- Find IPs claimed by more than one MAC in the same window
SELECT sender_ip, COUNT(DISTINCT sender_mac) AS mac_count,
       COLLECT_SET(sender_mac) AS macs
FROM arp_packets
WHERE arp_opcode = 2   -- ARP reply
GROUP BY sender_ip, window(timestamp, '10 seconds')
HAVING mac_count > 1
```

### Step 4 – Detection Logic for ARP Attacks

Because you know the capture contains an ARP attack, you can implement **rule‑based detectors** that mirror what security tools look for:

**a) Classic ARP Spoofing / Man‑in‑the‑Middle**

- A host sends an ARP reply mapping the gateway’s IP to its own MAC, while sending another reply mapping the victim’s IP to its own MAC.
- **Detection rule**: An IP address (especially the default gateway) is associated with more than one MAC address over a short period OR a MAC address claims multiple IPs in a way that conflicts with historical baselines.

**b) Gratuitous ARP Storm**

- Unsolicited ARP replies (`arp.opcode == 2` and `target_ip == sender_ip`). A flood of these can indicate an attack tool (e.g., `arpspoof`).
- **Detection rule**: Count gratuitous ARP packets per second per host; raise alert if rate exceeds threshold.

**c) MAC Address Flapping**

- A single IP repeatedly changes its MAC.
- **Detection rule**: For any IP, if the set of associated MACs changes more than N times in a time window, flag it.

In Spark, implement these as DataFrame queries and store flagged events in a “suspicious_activity” table/DataFrame.

### Step 5 – Machine Learning (Optional, but Adds Depth)

If you want to show an ML‑based approach, you can train an anomaly detector. Because you have labelled attack data (the capture is “attack”), you can either:

- Use unsupervised learning (e.g., Isolation Forest, K‑Means) to find outliers among normal ARP behaviour, then verify they correspond to the attack.
- Engineer features like packet rate, ARP reply/request ratio, unique IP‑MAC pair count per window, and apply Spark MLlib’s **KMeans** or **IsolationForest** (available via third‑party or custom implementation).

Spark MLlib pipeline example:

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

# Assemble features per time window
assembler = VectorAssembler(inputCols=["reply_count", "request_count", "unique_mac_per_ip"], outputCol="features")
feat_df = assembler.transform(windowed_stats)

kmeans = KMeans(k=2, seed=1)
model = kmeans.fit(feat_df)
predictions = model.transform(feat_df)
# The smaller cluster often contains the anomalous bursts.
```

### Step 6 – Visualisation & Reporting

You can present results directly in a Jupyter notebook using matplotlib/seaborn, or build a small dashboard.

- **Jupyter**: Plot timeline of suspicious events, bar charts of attacker MAC activity.
- **Apache Zeppelin**: Native Spark integration, good for live demos.
- **Elasticsearch + Kibana**: Write flagged events from Spark to Elasticsearch using `elasticsearch-hadoop` connector and create dashboards.

### Step 7 – (Future) Scaling to Real‑Time

Once the batch pipeline works, you can adapt it to streaming by replacing the static PCAP source with Kafka + Spark Structured Streaming. This demonstrates a complete security information and event management (SIEM) mini‑system.

---

## 3. Tools & Libraries You Can Utilise

| Layer                | Recommended Tool / Library                          |
|----------------------|-----------------------------------------------------|
| PCAP parsing         | Tshark, Scapy, DPKT, Pyshark                         |
| Data storage         | Hadoop HDFS, AWS S3 (simulated via MinIO), Parquet  |
| Processing engine    | Apache Spark (PySpark)                              |
| Interactive analysis | Jupyter Notebook, Apache Zeppelin                   |
| ML                   | Spark MLlib, scikit‑learn (if data small), PyOD      |
| Visualisation        | Matplotlib, Seaborn, Kibana, Plotly                  |
| Orchestration        | Apache Airflow (optional, to schedule the pipeline)  |

---

## 4. What You Can Demonstrate as Final Deliverables

1. **Data pipeline** that ingests raw PCAP, transforms, and stores it efficiently in HDFS/Parquet.
2. **Detection module** that outputs a list of suspicious IP‑MAC pairs with timestamps and attack type (e.g., “ARP spoofing attempt from 08:00:27:ab:cd:ef”).
3. **Metrics**: number of ARP packets processed per second, anomaly scores.
4. **Dashboard** (even static plots) showing the attack window and highlighting the malicious node.
5. **Performance evaluation** – how Spark scales with larger synthetic PCAP files (you can use tools like `tcpreplay` or `scapy` to generate bigger datasets).

---

## 5. Suggested Project Narrative

> “I built a scalable network security analytics system on Hadoop/Spark. Using a Wireshark capture of an ARP poisoning attack, I parsed raw packets, stored them in a data lake, and implemented both rule‑based and machine‑learning detectors to automatically flag malicious ARP activity. The pipeline can easily be extended to live traffic and other protocols, demonstrating how big data technologies can power next‑generation intrusion detection.”

This structure gives you a clear map from raw capture to a full‑blown big data project, covering storage, processing, analytics, and visualisation – all while delivering meaningful security insights.
