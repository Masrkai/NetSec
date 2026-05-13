import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, coalesce, lit, expr, approx_count_distinct, lower
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ----------------------------------------------------------------------
# 1. Spark session (with local filesystem override)
# ----------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("ARP Anomaly Detection") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-ckpt/global") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ----------------------------------------------------------------------
# 2. Schema & Streaming Source
# ----------------------------------------------------------------------
CKPT_BASE = "/tmp/spark-ckpt"
if os.path.exists(CKPT_BASE):
    shutil.rmtree(CKPT_BASE)

# ✅ Use triple-slash file:// URL for absolute local paths
CSV_DIR = f"file://{os.path.abspath('./Captures/CSV')}"  # Becomes: file:///home/.../Captures/CSV
print(f"📂 Streaming source resolved to: {CSV_DIR}")

# Optional: Verify path exists before starting
if not os.path.isdir(os.path.abspath('./Captures/CSV')):
    raise FileNotFoundError(f"CSV directory not found: {os.path.abspath('./Captures/CSV')}")

arp_schema = StructType([
    StructField("frame.time_epoch", DoubleType(), True),
    StructField("eth.src", StringType(), True),
    StructField("eth.dst", StringType(), True),
    StructField("arp.opcode", IntegerType(), True),
    StructField("arp.src.hw_mac", StringType(), True),
    StructField("arp.src.proto_ipv4", StringType(), True),
    StructField("arp.dst.hw_mac", StringType(), True),
    StructField("arp.dst.proto_ipv4", StringType(), True),
    StructField("arp.isgratuitous", StringType(), True)
])

raw_stream = spark.readStream \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .option("pathGlobFilter", "*.csv") \
    .option("recursiveFileLookup", "false") \
    .schema(arp_schema) \
    .csv(CSV_DIR)

# Clean names, parse boolean safely, set watermark EXACTLY ONCE
arp_stream = raw_stream \
    .selectExpr(
        "cast(`frame.time_epoch` as double) as ts",
        "`eth.src` as eth_src",
        "`eth.dst` as eth_dst",
        "`arp.opcode` as opcode",
        "`arp.src.hw_mac` as arp_src_mac",
        "`arp.src.proto_ipv4` as arp_src_ip",
        "`arp.dst.hw_mac` as arp_dst_mac",
        "`arp.dst.proto_ipv4` as arp_dst_ip",
        "`arp.isgratuitous` as isgratuitous_raw"
    ) \
    .withColumn("event_ts", col("ts").cast("timestamp")) \
    .withColumn("isgratuitous", 
        lower(col("isgratuitous_raw")).isin(["true", "1", "yes"])) \
    .withWatermark("event_ts", "15 seconds")
# ----------------------------------------------------------------------
# 3. ARP Scanning Detection
# ----------------------------------------------------------------------
scanner_alerts = arp_stream \
    .filter(col("opcode") == 1) \
    .groupBy(window(col("event_ts"), "10 seconds", "5 seconds"), col("eth_src")) \
    .agg(approx_count_distinct("arp_dst_ip").alias("unique_dst")) \
    .filter(col("unique_dst") > 20) \
    .selectExpr("window.start as window_start", "window.end as window_end", 
                "eth_src as scanner_mac", "unique_dst as targets_probed")

query_scan = scanner_alerts.writeStream \
    .outputMode("update").format("console").option("truncate", "false") \
    .option("checkpointLocation", "/tmp/spark-ckpt/scan") \
    .queryName("ARP Scanner").start()

# ----------------------------------------------------------------------
# 4. Gratuitous ARP Storm Detection
# ----------------------------------------------------------------------
# garp_alerts = arp_stream \
#     .filter(col("isgratuitous") == True) \
#     .groupBy(window(col("event_ts"), "10 seconds"), col("eth_src")) \
#     .count().filter(col("count") > 5) \
#     .selectExpr("window.start as window_start", "window.end as window_end", 
#                 "eth_src as mac", "count as garp_count")

# 1. GARP: Lower threshold or make it configurable
garp_alerts = arp_stream \
    .filter(col("isgratuitous") == True) \
    .groupBy(window(col("event_ts"), "60 seconds"), col("eth_src")) \
    .count() \
    .filter(col("count") > 0)  # Any GARP is worth noting

query_garp = garp_alerts.writeStream \
    .outputMode("update").format("console").option("truncate", "false") \
    .option("checkpointLocation", "/tmp/spark-ckpt/garp") \
    .queryName("GARP Storm").start()

# ----------------------------------------------------------------------
# 5. ARP Spoofing Detection
# ----------------------------------------------------------------------
spoof_alerts = arp_stream \
    .filter((col("opcode") == 1) & (col("eth_src") != col("arp_src_mac"))) \
    .selectExpr("event_ts", "eth_src as real_mac", "arp_src_mac as claimed_mac", 
                "arp_src_ip as claimed_ip", "arp_dst_ip as target_ip")


query_spoof = spoof_alerts.writeStream \
    .outputMode("append").format("console").option("truncate", "false") \
    .option("checkpointLocation", "/tmp/spark-ckpt/spoof") \
    .queryName("ARP Spoofing").start()

# ----------------------------------------------------------------------
# 6. ARP Request Flood Detection
# ----------------------------------------------------------------------
flood_alerts = arp_stream \
    .filter(col("opcode") == 1) \
    .groupBy(window(col("event_ts"), "1 second"), col("eth_src")) \
    .count().filter(col("count") > 100) \
    .selectExpr("window.start as second", "eth_src as mac", "count as requests_per_sec")

query_flood = flood_alerts.writeStream \
    .outputMode("update").format("console").option("truncate", "false") \
    .option("checkpointLocation", "/tmp/spark-ckpt/flood") \
    .queryName("ARP Request Flood").start()

# ----------------------------------------------------------------------
# 7. Unsolicited ARP Reply Detection (Fixed Watermark Inheritance)
# ----------------------------------------------------------------------
requests = arp_stream \
    .filter(col("opcode") == 1) \
    .selectExpr("event_ts as req_ts", "arp_src_ip as req_src_ip", 
                "arp_dst_ip as req_dst_ip", "eth_src as req_mac")
    # ✅ Watermark inherited from arp_stream. Do NOT call .withWatermark() again.

replies = arp_stream \
    .filter(col("opcode") == 2) \
    .selectExpr("event_ts as rep_ts", "arp_src_ip as rep_src_ip", 
                "arp_dst_ip as rep_dst_ip", "eth_src as rep_mac")
    # ✅ Watermark inherited

join_condition = expr("""
    rep_src_ip = req_dst_ip AND
    rep_dst_ip = req_src_ip AND
    rep_ts >= req_ts AND
    rep_ts <= req_ts + interval 5 seconds
""")

joined = replies.join(requests, join_condition, "leftOuter")

unsolicited = joined \
    .filter(col("req_ts").isNull()) \
    .selectExpr("rep_ts as time", "rep_mac as mac", "rep_src_ip as ip", 
                "'Unsolicited ARP reply' as alert")

query_unsol = unsolicited.writeStream \
    .outputMode("append").format("console").option("truncate", "false") \
    .option("checkpointLocation", "/tmp/spark-ckpt/unsol") \
    .queryName("Unsolicited Reply").start()

# ----------------------------------------------------------------------
# 8. MAC Impersonation Detection
# ----------------------------------------------------------------------
mac_flood_alerts = arp_stream \
    .filter(col("opcode").isin(1, 2)) \
    .groupBy(window(col("event_ts"), "10 seconds"), col("eth_src")) \
    .agg(approx_count_distinct("arp_src_ip").alias("unique_ips")) \
    .filter(col("unique_ips") > 5) \
    .selectExpr("window.start as window_start", "window.end as window_end", 
                "eth_src as mac", "unique_ips as ip_count")

query_mac = mac_flood_alerts.writeStream \
    .outputMode("update").format("console").option("truncate", "false") \
    .option("checkpointLocation", "/tmp/spark-ckpt/mac") \
    .queryName("MAC Flooding").start()

# ----------------------------------------------------------------------
# 9. Await Termination
# ----------------------------------------------------------------------
print("✅ All detectors started. Waiting for data...")
spark.streams.awaitAnyTermination()