from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, window, count, countDistinct,
    collect_set, size, struct, expr,
    min as spark_min, max as spark_max
)

from config import ARPConfig

# =============================================================================
# DETECTOR 1: ARP SCANNING
# =============================================================================

def detect_arp_scanning(df: DataFrame, config: ARPConfig) -> DataFrame:
    return (
        df.filter(col("is_request") & col("is_broadcast"))
        .groupBy(
            window(
                col("event_ts"),
                f"{config.SCAN_WINDOW_SECONDS} seconds",
                f"{config.SCAN_SLIDE_SECONDS} seconds",
            ),
            col("eth_src"),
        )
        .agg(
            countDistinct("arp_dst_ip").alias("unique_targets"),
            count("*").alias("total_requests"),
            collect_set("arp_dst_ip").alias("targeted_ips"),
            spark_min("event_ts").alias("window_first_ts"),
            spark_max("event_ts").alias("window_last_ts"),
        )
        .filter(col("unique_targets") >= config.SCAN_UNIQUE_TARGETS_THRESHOLD)
        .selectExpr(
            "window.start as window_start",
            "window.end as window_end",
            "eth_src as scanner_mac",
            "unique_targets",
            "total_requests",
            "size(targeted_ips) as targeted_ip_count",
            "window_first_ts",
            "window_last_ts",
        )
    )

# =============================================================================
# DETECTOR 2: GARP ACTIVITY
# =============================================================================

def detect_garp_activity(df: DataFrame, config: ARPConfig) -> DataFrame:
    return (
        df.filter(col("is_gratuitous") == True)
        .groupBy(
            window(col("event_ts"), f"{config.GARP_WINDOW_SECONDS} seconds"),
            col("eth_src"),
        )
        .agg(
            count("*").alias("garp_count"),
            collect_set("arp_src_ip").alias("claimed_ips"),
            spark_min("event_ts").alias("first_garp"),
            spark_max("event_ts").alias("last_garp"),
        )
        .filter(col("garp_count") >= config.GARP_COUNT_THRESHOLD)
        .selectExpr(
            "window.start as window_start",
            "window.end as window_end",
            "eth_src as mac",
            "garp_count",
            "size(claimed_ips) as claimed_ip_count",
            "first_garp",
            "last_garp",
        )
    )

# =============================================================================
# DETECTOR 3: ARP SPOOFING
# =============================================================================

def detect_arp_spoofing(df: DataFrame, config: ARPConfig):
    # Heuristic A: MAC flipping
    mac_flipping = (
        df.filter(
            (col("is_reply") | col("is_gratuitous"))
            & col("arp_src_mac").isNotNull()
            & (col("arp_src_mac") != "00:00:00:00:00:00")
        )
        .groupBy(
            window(col("event_ts"), f"{config.IMPERSONATION_WINDOW_SECONDS} seconds"),
            col("eth_src"),
        )
        .agg(
            countDistinct("arp_src_mac").alias("unique_arp_macs"),
            countDistinct("arp_src_ip").alias("unique_arp_ips"),
            collect_set(struct("arp_src_mac", "arp_src_ip")).alias("claimed_identities"),
            count("*").alias("packet_count"),
        )
        .filter(col("unique_arp_macs") > 1)
        .selectExpr(
            "window.start as window_start",
            "window.end as window_end",
            "eth_src as eth_mac",
            "unique_arp_macs",
            "unique_arp_ips",
            "size(claimed_identities) as identity_count",
            "packet_count",
            "'MAC flipping' as spoof_heuristic",
        )
    )

    # Heuristic B: L2/L3 mismatch in replies
    reply_mismatch = df.filter(col("is_reply") & col("has_mac_mismatch")).selectExpr(
        "event_ts",
        "eth_src as real_mac",
        "arp_src_mac as claimed_mac",
        "arp_src_ip as claimed_ip",
        "arp_dst_ip as target_ip",
        "'Reply MAC mismatch' as spoof_heuristic",
    )

    # Heuristic C: Rapid IP claiming
    ip_flipping = (
        df.filter(
            (col("is_reply") | col("is_gratuitous") | col("is_request"))
            & col("arp_src_ip").isNotNull()
            & (col("arp_src_ip") != "0.0.0.0")
        )
        .groupBy(
            window(col("event_ts"), f"{config.IMPERSONATION_WINDOW_SECONDS} seconds"),
            col("eth_src"),
        )
        .agg(
            countDistinct("arp_src_ip").alias("unique_src_ips"),
            collect_set("arp_src_ip").alias("claimed_ips"),
            count("*").alias("packet_count"),
        )
        .filter(col("unique_src_ips") >= config.IMPERSONATION_IP_COUNT_THRESHOLD)
        .selectExpr(
            "window.start as window_start",
            "window.end as window_end",
            "eth_src as mac",
            "unique_src_ips",
            "size(claimed_ips) as ip_count",
            "packet_count",
            "'IP flipping' as spoof_heuristic",
        )
    )

    return reply_mismatch, mac_flipping, ip_flipping

# =============================================================================
# DETECTOR 4: REQUEST FLOOD
# =============================================================================

def detect_request_flood(df: DataFrame, config: ARPConfig) -> DataFrame:
    return (
        df.filter(col("is_request"))
        .groupBy(
            window(col("event_ts"), f"{config.FLOOD_WINDOW_SECONDS} seconds"),
            col("eth_src"),
        )
        .agg(
            count("*").alias("requests_per_sec"),
            countDistinct("arp_dst_ip").alias("unique_targets"),
            spark_min("event_ts").alias("second_start"),
            spark_max("event_ts").alias("second_end"),
        )
        .filter(col("requests_per_sec") >= config.FLOOD_REQUESTS_PER_SEC)
        .selectExpr(
            "window.start as second",
            "eth_src as mac",
            "requests_per_sec",
            "unique_targets",
            "second_start",
            "second_end",
        )
    )

# =============================================================================
# DETECTOR 5: UNSOLICITED ARP REPLY
# =============================================================================

def detect_unsolicited_replies(df: DataFrame, config: ARPConfig) -> DataFrame:
    requests = df.filter(col("is_request")).select(
        col("event_ts").alias("req_ts"),
        col("arp_src_ip").alias("req_src_ip"),
        col("arp_dst_ip").alias("req_dst_ip"),
        col("eth_src").alias("req_eth_src"),
        col("arp_src_mac").alias("req_src_mac"),
    )

    replies = df.filter(col("is_reply")).select(
        col("event_ts").alias("rep_ts"),
        col("arp_src_ip").alias("rep_src_ip"),
        col("arp_dst_ip").alias("rep_dst_ip"),
        col("eth_src").alias("rep_eth_src"),
        col("arp_src_mac").alias("rep_src_mac"),
    )

    matched = replies.join(
        requests,
        on=expr(f"""
            rep_src_ip = req_dst_ip AND
            rep_dst_ip = req_src_ip AND
            rep_ts >= req_ts AND
            rep_ts <= req_ts + interval {config.UNSOLICITED_LOOKBACK_SECONDS} seconds
        """),
        how="left",
    )

    unsolicited = matched.filter(col("req_ts").isNull()).selectExpr(
        "rep_ts as time",
        "rep_eth_src as mac",
        "rep_src_ip as ip",
        "rep_src_mac as advertised_mac",
        "'Unsolicited ARP reply' as alert",
        "'No matching request in lookback window' as reason",
    )

    return unsolicited

# =============================================================================
# DETECTOR 6: MAC IMPERSONATION
# =============================================================================

def detect_mac_impersonation(df: DataFrame, config: ARPConfig) -> DataFrame:
    return (
        df.filter(col("is_request") | col("is_reply"))
        .groupBy(
            window(col("event_ts"), f"{config.IMPERSONATION_WINDOW_SECONDS} seconds"),
            col("eth_src"),
        )
        .agg(
            countDistinct("arp_src_ip").alias("unique_src_ips"),
            countDistinct("arp_src_mac").alias("unique_arp_macs"),
            collect_set(struct("arp_src_ip", "arp_src_mac")).alias("claimed_identities"),
            count("*").alias("total_packets"),
        )
        .filter(
            (col("unique_src_ips") >= config.IMPERSONATION_IP_COUNT_THRESHOLD)
            | (col("unique_arp_macs") > 1)
        )
        .selectExpr(
            "window.start as window_start",
            "window.end as window_end",
            "eth_src as mac",
            "unique_src_ips",
            "unique_arp_macs",
            "size(claimed_identities) as identity_count",
            "total_packets",
            "'MAC impersonation' as alert_type",
        )
    )

# =============================================================================
# DETECTOR 7: ARP CONFLICT
# =============================================================================

def detect_arp_conflicts(df: DataFrame, config: ARPConfig) -> DataFrame:
    return (
        df.filter(
            (col("is_reply") | col("is_gratuitous"))
            & col("arp_src_ip").isNotNull()
            & (col("arp_src_ip") != "0.0.0.0")
        )
        .groupBy(
            window(col("event_ts"), f"{config.CONFLICT_WINDOW_SECONDS} seconds"),
            col("arp_src_ip"),
        )
        .agg(
            countDistinct("arp_src_mac").alias("claiming_mac_count"),
            collect_set("arp_src_mac").alias("claiming_macs"),
            count("*").alias("claim_count"),
        )
        .filter(col("claiming_mac_count") > 1)
        .selectExpr(
            "window.start as window_start",
            "window.end as window_end",
            "arp_src_ip as contested_ip",
            "claiming_mac_count",
            "size(claiming_macs) as mac_count",
            "claim_count",
            "'IP conflict' as conflict_type",
        )
    )