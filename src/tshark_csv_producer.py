
#!/usr/bin/env python3
"""
tshark_csv_producer.py
======================
Captures live ARP packets and writes rolling CSV files to a directory
for Spark Structured Streaming to consume.
"""
import argparse
import csv
import os
import sys
import signal
import subprocess
import tempfile
import threading
import time
from typing import List

from schema import ARP_FIELD_METADATA


def build_tshark_command(interface: str, field_names: List[str]) -> List[str]:
    cmd = [
        "tshark", "-l", "-q", "-n", "-i", interface,
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


class TSharkCSVProducer:
    def __init__(self, interface: str = "eth0", output_dir: str = "/tmp/arp_stream",
                 rollover_every: int = 50):
        self.interface = interface
        self.output_dir = output_dir
        self.rollover_every = rollover_every
        self._shutdown = False
        self._proc = None
        os.makedirs(output_dir, exist_ok=True)

    def run(self):
        field_names = [name for name, _, _, _ in ARP_FIELD_METADATA]
        cmd = build_tshark_command(self.interface, field_names)

        print(f"[PRODUCER] tshark {self.interface} → CSV dir {self.output_dir}")
        print(f"[PRODUCER] Rolling every {self.rollover_every} ARP packets")
        print("[PRODUCER] Press Ctrl+C to stop")

        self._proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, bufsize=1)
        reader = csv.DictReader(self._proc.stdout)

        batch = []
        batch_num = 0

        try:
            for row in reader:
                if self._shutdown:
                    break
                batch.append(row)
                if len(batch) >= self.rollover_every:
                    self._write_batch(batch, batch_num)
                    batch = []
                    batch_num += 1
            if batch:
                self._write_batch(batch, batch_num)
        except Exception as exc:
            print(f"[PRODUCER] Error: {exc}", file=sys.stderr)
        finally:
            self.stop()

    def _write_batch(self, rows, batch_num):
        filepath = os.path.join(self.output_dir, f"arp_{batch_num:06d}.csv")
        field_names = [name for name, _, _, _ in ARP_FIELD_METADATA]
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        print(f"[PRODUCER] Wrote {len(rows)} rows → {filepath}")

    def stop(self):
        if self._shutdown:
            return
        self._shutdown = True
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        print("[PRODUCER] Stopped.")


def main():
    parser = argparse.ArgumentParser(description="ARP tshark → rolling CSV")
    parser.add_argument("--iface", default="eth0")
    parser.add_argument("--dir", default="/tmp/arp_stream")
    parser.add_argument("--batch-size", type=int, default=50)
    args = parser.parse_args()

    producer = TSharkCSVProducer(args.iface, args.dir, args.batch_size)

    def _shutdown(*_):
        producer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    producer.run()


if __name__ == "__main__":
    main()