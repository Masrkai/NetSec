import os
import shutil
from typing import Optional, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from config import ARPConfig
from data_source import ARPDataSource
from enrichment import enrich_arp_data
from detector_registry import STANDARD_DETECTORS, SPOOFING


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

    for idx, spec in enumerate(STANDARD_DETECTORS, 1):
        print(f"\n[{idx}] {spec.name} Detection...")
        df = spec.fn(enriched, config)
        results[spec.name.lower()] = df
        cnt = df.count()
        print(f"      Found {cnt} events")
        if cnt > 0:
            df.show(truncate=False)

    print(f"\n[{len(STANDARD_DETECTORS)+1}] ARP Spoofing Detection (3 heuristics)...")
    reply_mismatch, mac_flipping, ip_flipping = SPOOFING.fn(enriched, config)
    results["spoof_reply_mismatch"] = reply_mismatch
    results["spoof_mac_flipping"] = mac_flipping
    results["spoof_ip_flipping"] = ip_flipping

    for sub_name, df in zip(SPOOFING.sub_names, (reply_mismatch, mac_flipping, ip_flipping)):
        cnt = df.count()
        print(f"      {sub_name}: {cnt}")
        if cnt > 0:
            df.show(truncate=False)

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

    for spec in STANDARD_DETECTORS:
        df = results.get(spec.name.lower())
        if df is not None:
            cnt = df.count()
            status = "🔴 ALERT" if cnt > 0 else "🟢 Clean"
            print(f"  {spec.name:25s}: {cnt:3d} events {status}")

    for sub_name in SPOOFING.sub_names:
        key = f"spoof_{sub_name.lower().replace(' ', '_')}"
        df = results.get(key)
        if df is not None:
            cnt = df.count()
            status = "🔴 ALERT" if cnt > 0 else "🟢 Clean"
            print(f"  Spoof ({sub_name:15s}): {cnt:3d} events {status}")

    enriched.unpersist()
    return results