from dataclasses import dataclass

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

    # Future Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_ARP: str = "network-arp-raw"