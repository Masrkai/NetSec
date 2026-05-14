from dataclasses import dataclass, field
from typing import Dict

@dataclass
class ARPConfig:
    """Centralized configuration for all detectors."""

    # Time windows
    SCAN_WINDOW_SECONDS: int = 10
    SCAN_SLIDE_SECONDS: int = 5
    FLOOD_WINDOW_SECONDS: int = 1
    GARP_WINDOW_SECONDS: int = 60
    IMPERSONATION_WINDOW_SECONDS: int = 30
    CONFLICT_WINDOW_SECONDS: int = 10
    UNSOLICITED_LOOKBACK_SECONDS: int = 5

    # Thresholds
    SCAN_UNIQUE_TARGETS_THRESHOLD: int = 15
    FLOOD_REQUESTS_PER_SEC: int = 50
    GARP_COUNT_THRESHOLD: int = 1
    IMPERSONATION_IP_COUNT_THRESHOLD: int = 2

    # Paths
    CHECKPOINT_BASE: str = "/tmp/spark-ckpt-arp"
    CSV_INPUT_DIR: str = "./Captures/CSV"
    OUTPUT_BASE: str = "/tmp/arp_output"

    # Streaming config
    MAX_FILES_PER_TRIGGER: int = 1
    WATERMARK_SECONDS: int = 15

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_ARP: str = "network-arp-raw"

    # Threat scoring weights (shared across UI and live scoring)
    THREAT_WEIGHTS: Dict[str, int] = field(default_factory=lambda: {
        "scanning": 12,
        "flood": 25,
        "reply_mismatch": 18,
        "mac_flipping": 20,
        "ip_flipping": 12,
        "unsolicited": 14,
        "impersonation": 14,
        "conflict": 10,
        "garp": 4,
    })