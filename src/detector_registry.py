from dataclasses import dataclass
from typing import Callable, List, Tuple
from pyspark.sql import DataFrame

from config import ARPConfig
from detectors import (
    detect_arp_scanning,
    detect_garp_activity,
    detect_request_flood,
    detect_mac_impersonation,
    detect_arp_conflicts,
    detect_unsolicited_replies,
    detect_arp_spoofing,
)


@dataclass(frozen=True)
class DetectorSpec:
    name: str
    fn: Callable[[DataFrame, ARPConfig], DataFrame]
    output_mode: str


@dataclass(frozen=True)
class SpoofingSpec:
    name: str
    fn: Callable[[DataFrame, ARPConfig], Tuple[DataFrame, DataFrame, DataFrame]]
    sub_names: Tuple[str, ...]
    output_modes: Tuple[str, ...]


STANDARD_DETECTORS: List[DetectorSpec] = [
    DetectorSpec("Scanning", detect_arp_scanning, "update"),
    DetectorSpec("GARP", detect_garp_activity, "update"),
    DetectorSpec("Flood", detect_request_flood, "update"),
    DetectorSpec("Impersonation", detect_mac_impersonation, "update"),
    DetectorSpec("Conflict", detect_arp_conflicts, "update"),
    DetectorSpec("Unsolicited", detect_unsolicited_replies, "append"),
]

SPOOFING = SpoofingSpec(
    "Spoofing",
    detect_arp_spoofing,
    ("Reply Mismatch", "MAC Flipping", "IP Flipping"),
    ("append", "update", "update"),
)