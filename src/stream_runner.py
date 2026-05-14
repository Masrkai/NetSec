import os
import shutil
from typing import Optional, List

from pyspark.sql import SparkSession

from config import ARPConfig
from data_source import ARPDataSource
from enrichment import enrich_arp_data
from detector_registry import STANDARD_DETECTORS, SPOOFING


def run_streaming_analysis(
    spark: SparkSession,
    config: ARPConfig,
    csv_dir: Optional[str] = None,
    use_kafka: bool = False,
) -> List:
    """Run all detectors in streaming mode using foreachBatch."""

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
    queries = []

    print("[INFO] Starting detector streams...")

    # ------------------------------------------------------------------
    # STANDARD DETECTORS
    # ------------------------------------------------------------------
    def make_handler(spec):
        def handler(batch_df, batch_id):
            if batch_df.rdd.isEmpty():
                return
            try:
                result = spec.fn(batch_df, config)
                cnt = result.count()
                if cnt > 0:
                    print(f"\n🔴 [{spec.name}] {cnt} events in micro-batch {batch_id}")
                    result.show(truncate=False)
                else:
                    print(f"🟢 [{spec.name}] Clean in micro-batch {batch_id}")
            except Exception as e:
                print(f"⚠️  [{spec.name}] Handler error: {e}")
        return handler

    for spec in STANDARD_DETECTORS:
        q = (
            enriched.writeStream
            .foreachBatch(make_handler(spec))
            .outputMode("append")
            .queryName(spec.name)
            .start()
        )
        queries.append(q)

    # ------------------------------------------------------------------
    # SPOOFING (3 heuristics in one handler)
    # ------------------------------------------------------------------
    def spoofing_handler(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        try:
            reply_mismatch, mac_flipping, ip_flipping = SPOOFING.fn(batch_df, config)
            for df, sub_name in zip(
                (reply_mismatch, mac_flipping, ip_flipping),
                SPOOFING.sub_names,
            ):
                cnt = df.count()
                if cnt > 0:
                    print(f"\n🔴 [Spoof-{sub_name}] {cnt} events")
                    df.show(truncate=False)
        except Exception as e:
            print(f"⚠️  [Spoofing] Handler error: {e}")

    q = (
        enriched.writeStream
        .foreachBatch(spoofing_handler)
        .outputMode("append")
        .queryName("Spoofing")
        .start()
    )
    queries.append(q)

    print(f"[INFO] Started {len(queries)} streaming queries")
    print("[INFO] Waiting for data... Press Ctrl+C to stop")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[INFO] Stopping...")
        for q in queries:
            q.stop()

    return queries