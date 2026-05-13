import os
import shutil
from typing import Optional, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

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

def run_batch_analysis(
    spark: SparkSession, config: ARPConfig, csv_dir: Optional[str] = None
) -> Dict[str, DataFrame]:
    """Run all detectors in batch mode on historical CSV data."""

    print("=" * 60)
    print("ARP ANOMALY DETECTION — BATCH MODE (Stateless)")
    print("=" * 60)

    if os.path.exists(config.CHECKPOINT_BASE):
        shutil.rmtree(config.CHECKPOINT_BASE)
    os.makedirs(config.CHECKPOINT_BASE, exist_ok=True)

    source = ARPDataSource(spark, config)
    raw_df = source.read_csv_batch(csv_dir)

    total_raw = raw_df.count()
    print(f"[INFO] Loaded {total_raw} raw ARP packets")

    if total_raw == 0:
        print("[WARN] No data found. Exiting.")
        return {}

    enriched = enrich_arp_data(raw_df, config)
    enriched.cache()

    results = {}

    print("\n[1] ARP Scanning Detection...")
    results["scanning"] = detect_arp_scanning(enriched, config)
    cnt = results["scanning"].count()
    print(f"      Found {cnt} scanning events")
    if cnt > 0:
        results["scanning"].show(truncate=False)

    print("\n[2] GARP Activity Detection...")
    results["garp"] = detect_garp_activity(enriched, config)
    cnt = results["garp"].count()
    print(f"      Found {cnt} GARP events")
    if cnt > 0:
        results["garp"].show(truncate=False)

    print("\n[3] ARP Spoofing Detection (3 heuristics)...")
    reply_mismatch, mac_flipping, ip_flipping = detect_arp_spoofing(enriched, config)

    results["spoof_reply_mismatch"] = reply_mismatch
    cnt_rm = reply_mismatch.count()
    print(f"      Reply MAC mismatch: {cnt_rm}")
    if cnt_rm > 0:
        reply_mismatch.show(truncate=False)

    results["spoof_mac_flipping"] = mac_flipping
    cnt_mf = mac_flipping.count()
    print(f"      MAC flipping: {cnt_mf}")
    if cnt_mf > 0:
        mac_flipping.show(truncate=False)

    results["spoof_ip_flipping"] = ip_flipping
    cnt_if = ip_flipping.count()
    print(f"      IP flipping: {cnt_if}")
    if cnt_if > 0:
        ip_flipping.show(truncate=False)

    print("\n[4] Request Flood Detection...")
    results["flood"] = detect_request_flood(enriched, config)
    cnt = results["flood"].count()
    print(f"      Found {cnt} flood events")
    if cnt > 0:
        results["flood"].show(truncate=False)

    print("\n[5] Unsolicited Reply Detection...")
    results["unsolicited"] = detect_unsolicited_replies(enriched, config)
    cnt = results["unsolicited"].count()
    print(f"      Found {cnt} unsolicited replies")
    if cnt > 0:
        results["unsolicited"].show(truncate=False)

    print("\n[6] MAC Impersonation Detection...")
    results["impersonation"] = detect_mac_impersonation(enriched, config)
    cnt = results["impersonation"].count()
    print(f"      Found {cnt} impersonation events")
    if cnt > 0:
        results["impersonation"].show(truncate=False)

    print("\n[7] ARP Conflict Detection...")
    results["conflict"] = detect_arp_conflicts(enriched, config)
    cnt = results["conflict"].count()
    print(f"      Found {cnt} conflict events")
    if cnt > 0:
        results["conflict"].show(truncate=False)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    total_requests = enriched.filter(col("is_request")).count()
    total_replies = enriched.filter(col("is_reply")).count()
    total_garps = enriched.filter(col("is_gratuitous")).count()

    print(f"Total packets: {total_raw}")
    print(f"  Requests: {total_requests}")
    print(f"  Replies:  {total_replies}")
    print(f"  GARPs:    {total_garps}")
    print(f"\nDetections:")

    all_detections = [
        ("Scanning", results.get("scanning")),
        ("GARP", results.get("garp")),
        ("Spoof (reply mismatch)", results.get("spoof_reply_mismatch")),
        ("Spoof (MAC flipping)", results.get("spoof_mac_flipping")),
        ("Spoof (IP flipping)", results.get("spoof_ip_flipping")),
        ("Flood", results.get("flood")),
        ("Unsolicited", results.get("unsolicited")),
        ("Impersonation", results.get("impersonation")),
        ("Conflict", results.get("conflict")),
    ]

    for name, df in all_detections:
        if df is not None:
            cnt = df.count()
            status = "🔴 ALERT" if cnt > 0 else "🟢 Clean"
            print(f"  {name:25s}: {cnt:3d} events {status}")

    enriched.unpersist()
    return results