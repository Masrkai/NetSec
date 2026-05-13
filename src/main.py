#!/usr/bin/env python3
"""
ARP Anomaly Detection System — Stateless Version
=================================================
No reference tables. No persistence. Pure per-batch analysis.

Optimized for:
1. Current use: CSV file batch/streaming analysis (Wireshark/tshark exports)
2. Future use: Real-time ingestion from Kafka/network tap

Key design: Every detection is computed from the data in the current window only.
No cross-batch state, no baselines, no learning.
"""

import argparse

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

    args = parser.parse_args()

    config = ARPConfig()
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