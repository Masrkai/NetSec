import os
import shutil
from typing import Optional, List

from pyspark.sql import SparkSession

from config import ARPConfig
from data_source import ARPDataSource
from enrichment import enrich_arp_data
from detector_registry import STANDARD_DETECTORS, SPOOFING
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

    for spec in STANDARD_DETECTORS:
        df = spec.fn(enriched, config)
        queries.append(output.console_sink(df, spec.name, spec.output_mode))

    reply_mismatch, mac_flipping, ip_flipping = SPOOFING.fn(enriched, config)
    for df, sub_name, mode in zip(
        (reply_mismatch, mac_flipping, ip_flipping),
        SPOOFING.sub_names,
        SPOOFING.output_modes,
    ):
        queries.append(output.console_sink(df, f"Spoof-{sub_name.replace(' ', '')}", mode))

    print(f"[INFO] Started {len(queries)} streaming queries")
    print("[INFO] Waiting for data... Press Ctrl+C to stop")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[INFO] Stopping...")
        for q in queries:
            q.stop()

    return queries