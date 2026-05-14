## Phase 1 – Environment Setup & Initial Batch Analysis (Week 1)

### **Days 1‑2: Launch the Big‑Data Stack**

- Start a single‑node (or small cluster) with:
  - **Hadoop** (HDFS + YARN) – for storage and job scheduling.
  - **Apache Spark** (pre‑built for Hadoop, e.g., Spark 3.x) – for batch and streaming analytics.
  - **Apache Flume** (optional) – if you want to test live data ingestion later.
  - **Apache Kafka** (optional) – for real‑time streaming in Week 2.
- Verify access: `hdfs dfs -ls /`, `spark-shell`, etc.

### **Day 3: Ingest & Explore the Existing Data**

- Upload `arp_rich.csv` to HDFS:

  ```bash
  hdfs dfs -mkdir -p /data/arp/
  hdfs dfs -put arp_rich.csv /data/arp/
  ```

- Launch **Spark Shell** (or Jupyter) and load the data into a DataFrame:

  ```python
  # PySpark
  df = spark.read.option("header", "true").csv("/data/arp/arp_rich.csv")
  ```

- Explore the schema, timestamp conversion, null handling (the `arp.isgratuitous` column may have empty strings – map them to “false”).
- Register the DataFrame as a temporary view to run SQL queries:

  ```sql
  SELECT eth.src, count(*) as reqs
  FROM arp
  WHERE arp.opcode = '1'
  GROUP BY eth.src ORDER BY reqs DESC;
  ```

  You’ll immediately see `62:5e:34:e9:d2:73` topping the list – a clear indication of scanning.

### **Days 4‑5: Implement Batch Anomaly Detectors**

Write Spark jobs that output a list of suspicious events. Store results in HDFS (e.g., CSV or Parquet) for later reporting. Focus on four key detectors:

#### **1. ARP Scanner Detection**

- Use **Spark SQL**:

  ```sql
  SELECT window(timestamp, '10 seconds'), eth.src,
         COUNT(DISTINCT arp.dst.proto_ipv4) AS targets
  FROM arp
  WHERE arp.opcode = '1'
  GROUP BY window(timestamp, '10 seconds'), eth.src
  HAVING targets > 20
  ORDER BY window.start;
  ```

  (Convert `frame.time_epoch` to timestamp first.)

#### **2. Spoofed ARP Requests (MAC Mismatch)**

- Flag rows where `eth.src` ≠ `arp.src.hw_mac`:

  ```sql
  SELECT *
  FROM arp
  WHERE arp.opcode = '1' AND eth.src != arp.src.hw_mac;
  ```

  This catches the attacker trying to impersonate the router (`192.168.1.1` with MAC `44:fb:5a:d1:93:49` while using a different source MAC).

#### **3. Gratuitous ARP Storm**

- Count gARP per source MAC in a window:

  ```sql
  SELECT window(timestamp, '10 seconds'), eth.src, COUNT(*) AS garp_count
  FROM arp
  WHERE arp.isgratuitous = 'True'
  GROUP BY window(timestamp, '10 seconds'), eth.src
  HAVING garp_count > 3;
  ```

  The data shows `00:e0:4c:36:70:d5` sending multiple gratuitous ARP packets – a possible spoof preparation.

#### **4. Unsolicited ARP Replies**

- Identify replies (`opcode=2`) that are not preceded by a request from the same host within a short interval. A simple batch approach:  
  - All requests: `req` table with `src_mac`, `dst_ip`, `timestamp`.  
  - All replies: `rep` table with `dst_mac` (which is the original requester), `src_ip`.  
  - Left anti‑join replies against requests where `req.src_mac = rep.dst_mac` and `req.dst_ip = rep.src_ip` and time difference < 2 seconds. This shows replies without matching request.

Run each detector as a separate Spark job, export findings to JSON/CSV for the report.

### **Day 6‑7: Enrich, Visualize, and Document First Results**

- Create a summary table of all detected incidents:

  ```python
  all_alerts = scanner_alerts.unionByName(spoof_alerts).unionByName(garp_alerts)...
  all_alerts.write.mode("overwrite").csv("/data/output/alerts")
  ```

- If time permits, plot basic charts using a notebook (Matplotlib) or export data to a tool like Grafana.
- Start drafting the project report: describe the data, the anomalies found, and the Spark logic used.

---

## Phase 2 – Real‑Time Streaming & Finalization (Week 2)

### **Day 8‑9: Set Up a Live Feed (Optional but High Impact)**

- Use **Harbor** to generate new ARP attacks (spoof, flood, scan) on a test network interface while capturing with `tshark`.
- Pipe live ARP packets into **Kafka**:

  ```bash
  tshark -i eth0 -Y "arp" -T fields ... | kafka-console-producer --topic arp-live --broker-list ...
  ```

- Write a **Spark Structured Streaming** job that reads from Kafka, applies the same detectors in streaming fashion, and outputs alerts in real time.

  ```python
  stream_df = spark.readStream.format("kafka")...
  # apply windowed aggregations and filters as before
  alert_stream.writeStream.outputMode("update").format("console").start()
  ```

- This proves the system can handle real‑time network security monitoring.

### **Day 10: Test with Known Attack Patterns**

- Use Harbor’s attack profiles (e.g., ARP‑spoof, ARP‑scan) to trigger every detector you built.
- Confirm that all alerts fire correctly. Tune thresholds if needed.

### **Day 11: Integrate All Components**

- Ensure the end‑to‑end pipeline works:
  `Harbor simulation → tshark capture → Kafka → Spark Streaming → HDFS archive (for batch backup) + console/Kafka alerts`.
- Optionally, feed alerts into an **Elasticsearch** index and build a simple Kibana dashboard.

### **Day 12‑14: Finalize Documentation and Report**

- Document the architecture, the detection logic, and the results from both batch and streaming experiments.
- Include UML diagrams from your proposal, now updated with concrete components.
- Provide a set of recommendations: e.g., “Threshold of 20 unique IPs per second per source MAC successfully identified the scan performed at 1778013859”.

---

## Concrete Week‑by‑Week Deliverables

| Week | Deliverable |
|------|-------------|
| 1 | HDFS storage of `arp_rich.csv`, batch anomaly detectors (4 types), initial report with findings |
| 2 | Live ingestion pipeline (Kafka + Spark Streaming), integration with Harbor simulation, final report with performance metrics and recommendations |

---

## Mapping to Your Proposal

- **Hadoop Ecosystem:** You’ll use HDFS for storage, Spark for processing, and (optionally) Kafka/Flume for streaming.
- **Harbor & Wireshark:** Already provided the attack data; Week 2 will extend them to live testing.
- **UML Diagrams:** The actual flow you implement will mirror your proposed Mermaid diagrams, with Spark now sitting between HDFS and reporting.
- **Time constraint:** The plan focuses on core, demonstrable results; all optional parts (real‑time dashboard) are left for Week 2 if time allows.