#!/usr/bin/env python3
"""
tshark_producer.py
==================
Captures live ARP packets from an interface and publishes JSON records
to Kafka. Can be run standalone or imported as a module.
"""
import argparse
import csv
import json
import sys
import signal
import subprocess
from typing import Dict, Any, List

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from schema import ARP_FIELD_METADATA
from config import ARPConfig


def _safe_cast(value: str, typ: type, default: Any) -> Any:
    if value is None:
        return default
    try:
        if typ is int:
            return int(float(value))
        return typ(value)
    except (ValueError, TypeError):
        return default


def build_tshark_command(interface: str, field_names: List[str]) -> List[str]:
    """Construct tshark CLI for ARP field extraction."""
    cmd = [
        "tshark", "-l", "-i", interface,
        "-Y", "arp",
        "-T", "fields",
    ]
    for field in field_names:
        cmd.extend(["-e", field])
    cmd.extend([
        "-E", "header=y",
        "-E", "separator=,",
        "-E", "quote=d",
        "-E", "occurrence=f",
    ])
    return cmd


def row_to_payload(row: Dict[str, str]) -> Dict[str, Any]:
    """Convert a CSV DictReader row to a typed payload dict."""
    return {
        name: _safe_cast(row.get(name, "") or "", py_type, default)
        for name, _, py_type, default in ARP_FIELD_METADATA
    }


def ensure_topic(bootstrap_servers: str, topic: str) -> None:
    """Idempotent topic creation (safe for NixOS KRaft)."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        if topic not in admin.list_topics():
            admin.create_topics([
                NewTopic(name=topic, num_partitions=1, replication_factor=1)
            ])
            print(f"[PRODUCER] Created topic: {topic}")
        admin.close()
    except Exception as exc:
        print(f"[PRODUCER] Topic ensure warning (non-fatal): {exc}")


class TSharkProducer:
    """Encapsulated tshark → Kafka bridge."""

    def __init__(self, config: ARPConfig = None):
        self.config = config or ARPConfig()
        self._producer: KafkaProducer = None
        self._proc: subprocess.Popen = None
        self._shutdown = False

    def _make_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=5,
        )

    def run(self, interface: str = None) -> None:
        interface = interface or "eth0"
        field_names = [name for name, _, _, _ in ARP_FIELD_METADATA]

        ensure_topic(self.config.KAFKA_BOOTSTRAP_SERVERS, self.config.KAFKA_TOPIC_ARP)

        self._producer = self._make_producer()

        cmd = build_tshark_command(interface, field_names)
        print(
            f"[PRODUCER] tshark {interface} → "
            f"Kafka {self.config.KAFKA_BOOTSTRAP_SERVERS}/{self.config.KAFKA_TOPIC_ARP}"
        )
        print("[PRODUCER] Press Ctrl+C to stop")

        self._proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, bufsize=1)
        reader = csv.DictReader(self._proc.stdout)

        try:
            for row in reader:
                if self._shutdown:
                    break
                payload = row_to_payload(row)
                self._producer.send(self.config.KAFKA_TOPIC_ARP, payload)
        except Exception as exc:
            print(f"[PRODUCER] Error: {exc}", file=sys.stderr)
        finally:
            self.stop()

    def stop(self) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        if self._producer:
            self._producer.flush()
            self._producer.close()
        print("[PRODUCER] Flushed and exited.")


def main() -> None:
    parser = argparse.ArgumentParser(description="ARP tshark → Kafka bridge")
    parser.add_argument("--iface", default="eth0")
    parser.add_argument("--kafka", default=None, help="Override bootstrap servers")
    parser.add_argument("--topic", default=None, help="Override Kafka topic")
    args = parser.parse_args()

    config = ARPConfig()
    if args.kafka:
        config.KAFKA_BOOTSTRAP_SERVERS = args.kafka
    if args.topic:
        config.KAFKA_TOPIC_ARP = args.topic

    producer = TSharkProducer(config)

    def _shutdown(_signum, _frame):
        producer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    producer.run(args.iface)


if __name__ == "__main__":
    main()