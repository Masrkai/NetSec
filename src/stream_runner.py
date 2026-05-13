import os
import shutil
from typing import Optional, List

from pyspark.sql import SparkSession

from config import ARPConfig
from data_source import ARPDataSource
from enrichment import enrich_arp_data
from detectors import (
    detect_arp_scanning,
    detect_garp_activity,
    detect_arp_spoofing,
    detect_request_flood,
    detect_unsolicited_replies,
    detect_mac_impersonation,
    detect_arp_conflicts,
)
from output import OutputManager

def run_streaming_analysis(
    spark: SparkSession,
    config: ARPConfig,
    csv_dir: Optional[str] = None,
    use_kafka: bool = False,
) -> List:
    """Run all detectors in streaming mode."""

    print("=" * 60)
    print("ARP ANOMALY DETECTION — STREAMING MODE (Stateless)")
    print("=" * 60)

    if os.path.exists(config.CHECKPOINT_BASE):
        shutil.rmtree(config.CHECKPOINT_BASE)

    source = ARPDataSource(spark, config)
    if use_kafka:
        raw_stream = source.read_kafka_stream()
    else:
        raw_stream = source.read_csv_stream(csv_dir)

    enriched = enrich_arp_data(raw_stream, config)

    output = OutputManager(spark, config)
    queries = []

    print("[INFO] Starting detector streams...")

    scanning = detect_arp_scanning(enriched, config)
    queries.append(output.console_sink(scanning, "ARP-Scanner", "update"))

    garp = detect_garp_activity(enriched, config)
    queries.append(output.console_sink(garp, "GARP-Activity", "update"))

    reply_mismatch, mac_flipping, ip_flipping = detect_arp_spoofing(enriched, config)
    queries.append(output.console_sink(reply_mismatch, "Spoof-ReplyMismatch", "append"))
    queries.append(output.console_sink(mac_flipping, "Spoof-MACFlip", "update"))
    queries.append(output.console_sink(ip_flipping, "Spoof-IPFlip", "update"))

    flood = detect_request_flood(enriched, config)
    queries.append(output.console_sink(flood, "ARP-Flood", "update"))

    unsolicited = detect_unsolicited_replies(enriched, config)
    queries.append(output.console_sink(unsolicited, "Unsolicited-Reply", "append"))

    impersonation = detect_mac_impersonation(enriched, config)
    queries.append(output.console_sink(impersonation, "MAC-Impersonation", "update"))

    conflict = detect_arp_conflicts(enriched, config)
    queries.append(output.console_sink(conflict, "ARP-Conflict", "update"))

    print(f"[INFO] Started {len(queries)} streaming queries")
    print("[INFO] Waiting for data... Press Ctrl+C to stop")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[INFO] Stopping...")
        for q in queries:
            q.stop()

    return queries