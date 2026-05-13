from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, trim, from_unixtime

from config import ARPConfig

def enrich_arp_data(raw_df: DataFrame, config: ARPConfig) -> DataFrame:
    """Clean and enrich raw ARP data with computed fields."""

    df = raw_df.selectExpr(
        "cast(`frame.time_epoch` as double) as epoch_ts",
        "`eth.src` as eth_src_raw",
        "`eth.dst` as eth_dst_raw",
        "cast(`arp.opcode` as int) as opcode",
        "`arp.src.hw_mac` as arp_src_mac_raw",
        "`arp.src.proto_ipv4` as arp_src_ip_raw",
        "`arp.dst.hw_mac` as arp_dst_mac_raw",
        "`arp.dst.proto_ipv4` as arp_dst_ip_raw",
        "`arp.isgratuitous` as gratuitous_raw",
    )

    is_gratuitous = lower(trim(col("gratuitous_raw"))).isin(
        ["true", "1", "yes", "t", "y"]
    )

    df = df.withColumn("event_ts", from_unixtime(col("epoch_ts")).cast("timestamp"))

    df = (
        df.withColumn("eth_src", lower(trim(col("eth_src_raw"))))
        .withColumn("eth_dst", lower(trim(col("eth_dst_raw"))))
        .withColumn("arp_src_mac", lower(trim(col("arp_src_mac_raw"))))
        .withColumn("arp_src_ip", trim(col("arp_src_ip_raw")))
        .withColumn("arp_dst_mac", lower(trim(col("arp_dst_mac_raw"))))
        .withColumn("arp_dst_ip", trim(col("arp_dst_ip_raw")))
        .withColumn("is_gratuitous", is_gratuitous)
        .withColumn("is_broadcast", col("eth_dst") == "ff:ff:ff:ff:ff:ff")
        .withColumn("is_request", col("opcode") == 1)
        .withColumn("is_reply", col("opcode") == 2)
        .withColumn(
            "has_mac_mismatch",
            (col("eth_src") != col("arp_src_mac"))
            & col("arp_src_mac").isNotNull()
            & (col("arp_src_mac") != "00:00:00:00:00:00"),
        )
        .withWatermark("event_ts", f"{config.WATERMARK_SECONDS} seconds")
    )

    return df.select(
        "event_ts",
        "epoch_ts",
        "eth_src",
        "eth_dst",
        "opcode",
        "arp_src_mac",
        "arp_src_ip",
        "arp_dst_mac",
        "arp_dst_ip",
        "is_gratuitous",
        "is_broadcast",
        "is_request",
        "is_reply",
        "has_mac_mismatch",
    )