from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

def get_arp_schema() -> StructType:
    """Schema for raw ARP packet data from tshark/Wireshark CSV export."""
    return StructType([
        StructField("frame.time_epoch", DoubleType(), True),
        StructField("eth.src", StringType(), True),
        StructField("eth.dst", StringType(), True),
        StructField("arp.opcode", IntegerType(), True),
        StructField("arp.src.hw_mac", StringType(), True),
        StructField("arp.src.proto_ipv4", StringType(), True),
        StructField("arp.dst.hw_mac", StringType(), True),
        StructField("arp.dst.proto_ipv4", StringType(), True),
        StructField("arp.isgratuitous", StringType(), True),
    ])