# live_state.py
from collections import deque
import threading
from typing import Dict, List, Any
import pandas as pd

from config import ARPConfig


class LiveState:
    """Thread-safe bridge between Spark Streaming (writer) and Gradio (reader)."""

    def __init__(self):
        self._lock = threading.Lock()
        self._traffic = deque(maxlen=120)
        self._alerts = deque(maxlen=200)
        self._stats = {"total": 0, "req": 0, "rep": 0, "garp": 0, "threat": 0}
        self._active = False
        self._interface = "eth0"
        self._error = None

    def update_traffic(self, ts: str, req: int, rep: int, garp: int):
        with self._lock:
            self._traffic.append({"ts": ts, "req": req, "rep": rep, "garp": garp})

    def add_alerts(self, alert_type: str, pdf: pd.DataFrame):
        with self._lock:
            for _, row in pdf.iterrows():
                d = row.to_dict()
                d["_alert_type"] = alert_type
                d["_ts"] = pd.Timestamp("now").isoformat()
                self._alerts.appendleft(d)
            self._recompute_threat()

    def update_stats(self, total=None, req=None, rep=None, garp=None):
        with self._lock:
            if total is not None:
                self._stats["total"] = total
            if req is not None:
                self._stats["req"] = req
            if rep is not None:
                self._stats["rep"] = rep
            if garp is not None:
                self._stats["garp"] = garp

    def set_error(self, msg: str):
        with self._lock:
            self._error = msg

    def clear_error(self):
        with self._lock:
            self._error = None

    def _recompute_threat(self):
        def _norm(name: str) -> str:
            return name.lower().replace(" ", "_")

        score = sum(
            ARPConfig.THREAT_WEIGHTS.get(_norm(a.get("_alert_type", "")), 5)
            for a in self._alerts
        )
        self._stats["threat"] = min(score, 100)

    def get_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "traffic": list(self._traffic),
                "alerts": list(self._alerts)[:50],
                "stats": dict(self._stats),
                "active": self._active,
                "interface": self._interface,
                "error": self._error,
            }

    def set_active(self, val: bool, iface: str = None):
        with self._lock:
            self._active = val
            if iface:
                self._interface = iface