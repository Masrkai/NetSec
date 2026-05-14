#!/usr/bin/env python3
"""
ARP Anomaly Detection System — Stateless Version
"""

import argparse
import time
import threading
import tempfile
import shutil

from config import ARPConfig
from session import create_spark_session
from batch_runner import run_batch_analysis
from stream_runner import run_streaming_analysis


def main():
    parser = argparse.ArgumentParser(description="ARP Anomaly Detection (Stateless)")
    parser.add_argument(
        "--mode", choices=["batch", "stream"], default="batch", help="Execution mode"
    )
    parser.add_argument(
        "--csv-dir", default="./Captures/CSV", help="Input CSV directory"
    )
    parser.add_argument(
        "--kafka", action="store_true", help="Use Kafka for streaming (future)"
    )
    # NEW: live capture without Kafka or static CSV
    parser.add_argument(
        "--live", action="store_true", help="Live tshark → temp CSV → Spark streaming"
    )
    parser.add_argument(
        "--iface", default="eth0", help="Network interface for live capture"
    )
    parser.add_argument(
        "--live-batch-size", type=int, default=50,
        help="ARP packets per rolling CSV file"
    )

    args = parser.parse_args()
    config = ARPConfig()

    # ------------------------------------------------------------------
    # LIVE MODE: tshark → temp CSV → Spark
    # ------------------------------------------------------------------
    if args.live:
        from tshark_csv_producer import TSharkCSVProducer

        live_dir = tempfile.mkdtemp(prefix="arp_live_")
        print(f"[MAIN] Live capture temp dir: {live_dir}")

        producer = TSharkCSVProducer(
            interface=args.iface,
            output_dir=live_dir,
            rollover_every=args.live_batch_size,
        )

        producer_thread = threading.Thread(target=producer.run, daemon=True)
        producer_thread.start()

        # Let tshark open the interface before Spark starts polling
        time.sleep(2)

        spark = create_spark_session(streaming=True)

        try:
            queries = run_streaming_analysis(
                spark, config, csv_dir=live_dir, use_kafka=False
            )
        finally:
            print("[MAIN] Shutting down live capture...")
            producer.stop()
            producer_thread.join(timeout=5)
            spark.stop()
            shutil.rmtree(live_dir, ignore_errors=True)
        return

    # ------------------------------------------------------------------
    # NORMAL MODES
    # ------------------------------------------------------------------
    config.CSV_INPUT_DIR = args.csv_dir
    spark = create_spark_session(streaming=(args.mode == "stream"))

    try:
        if args.mode == "batch":
            results = run_batch_analysis(spark, config, args.csv_dir)
        else:
            queries = run_streaming_analysis(spark, config, args.csv_dir, args.kafka)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()