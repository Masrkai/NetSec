# live_runner.py
import os
import shutil
import threading
import time
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, sum as spark_sum

from config import ARPConfig
from session import create_spark_session
from data_source import ARPDataSource
from enrichment import enrich_arp_data
from detector_registry import STANDARD_DETECTORS, SPOOFING
from live_state import LiveState
from tshark_producer import TSharkProducer


class LiveOrchestrator:
    """Spawns tshark + runs Spark Structured Streaming over Kafka."""

    def __init__(self, state: LiveState, config: Optional[ARPConfig] = None):
        self.state = state
        self.config = config or ARPConfig()
        self.spark: Optional[SparkSession] = None
        self.queries = []
        self._producer: Optional[TSharkProducer] = None
        self._producer_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self, interface: str = "eth0", kafka_mode: bool = True):
        if self._thread and self._thread.is_alive():
            return
        self.state.clear_error()
        self.state.set_active(True, interface)
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, args=(interface, kafka_mode), daemon=True
        )
        self._thread.start()

    def _run(self, interface: str, kafka_mode: bool):
        try:
            if kafka_mode:
                self._start_tshark(interface)
            self._start_spark(kafka_mode)
        except Exception as exc:
            self.state.set_error(str(exc))
            self.state.set_active(False)

    def _start_tshark(self, interface: str):
        """Start producer in a daemon thread (encapsulated, no hardcoded CLI)."""
        self._producer = TSharkProducer(self.config)
        self._producer_thread = threading.Thread(
            target=self._producer.run,
            args=(interface,),
            daemon=True,
        )
        self._producer_thread.start()
        print(f"[ORCHESTRATOR] tshark producer thread started on {interface}")

    def _start_spark(self, kafka_mode: bool):
        if os.path.exists(self.config.CHECKPOINT_BASE):
            shutil.rmtree(self.config.CHECKPOINT_BASE)

        self.spark = create_spark_session(streaming=True)

        source = ARPDataSource(self.spark, self.config)
        if kafka_mode:
            raw = source.read_kafka_stream()
        else:
            raise ValueError("Only Kafka mode is supported for live capture")

        enriched = enrich_arp_data(raw, self.config)

        # ---- Traffic stats (1-second windows) ----
        traffic = (
            enriched.withWatermark("event_ts", f"{self.config.WATERMARK_SECONDS} seconds")
            .groupBy(window(col("event_ts"), "1 second"))
            .agg(
                count("*").alias("total"),
                spark_sum(col("is_request").cast("int")).alias("req"),
                spark_sum(col("is_reply").cast("int")).alias("rep"),
                spark_sum(col("is_gratuitous").cast("int")).alias("garp"),
            )
            .selectExpr("window.start as ts", "total", "req", "rep", "garp")
        )

        def update_traffic(batch_df, batch_id):
            if batch_df.count() == 0:
                return
            pdf = batch_df.toPandas()
            for _, row in pdf.iterrows():
                self.state.update_traffic(
                    str(row["ts"]),
                    int(row["req"] or 0),
                    int(row["rep"] or 0),
                    int(row["garp"] or 0),
                )
            self.state.update_stats(
                total=int(pdf["total"].sum()),
                req=int(pdf["req"].sum()),
                rep=int(pdf["rep"].sum()),
                garp=int(pdf["garp"].sum()),
            )

        q1 = (
            traffic.writeStream.foreachBatch(update_traffic)
            .outputMode("update")
            .queryName("traffic")
            .start()
        )
        self.queries.append(q1)

        # ---- Alert helpers ----
        def make_alert_handler(alert_type: str):
            def handler(batch_df, batch_id):
                cnt = batch_df.count()
                if cnt > 0:
                    self.state.add_alerts(alert_type, batch_df.toPandas())
            return handler

        # ---- Detection queries (DRY via registry) ----
        detectors = []
        for spec in STANDARD_DETECTORS:
            detectors.append((spec.fn(enriched, self.config), spec.name, spec.output_mode))

        reply_mismatch, mac_flipping, ip_flipping = SPOOFING.fn(enriched, self.config)
        for df, sub_name, mode in zip(
            (reply_mismatch, mac_flipping, ip_flipping),
            SPOOFING.sub_names,
            SPOOFING.output_modes,
        ):
            detectors.append((df, sub_name, mode))

        for df, name, out_mode in detectors:
            q = (
                df.writeStream.foreachBatch(make_alert_handler(name))
                .outputMode(out_mode)
                .queryName(name)
                .start()
            )
            self.queries.append(q)

        print(f"[ORCHESTRATOR] {len(self.queries)} streaming queries active")

        # Keep alive
        while not self._stop_event.is_set():
            time.sleep(0.5)

        # Cleanup
        print("[ORCHESTRATOR] Stopping queries...")
        for q in self.queries:
            try:
                q.stop()
            except Exception:
                pass
        if self.spark:
            self.spark.stop()
        self.queries.clear()

    def stop(self):
        print("[ORCHESTRATOR] Stop requested")
        self._stop_event.set()
        if self._producer:
            self._producer.stop()
        if self._producer_thread and self._producer_thread.is_alive():
            self._producer_thread.join(timeout=5)
        self.state.set_active(False)