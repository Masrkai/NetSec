from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from typing import List, Tuple, Any

# (tshark_field_name, spark_type, python_type, default_value)
ARP_FIELD_METADATA: List[Tuple[str, Any, type, Any]] = [
    ("frame.time_epoch", DoubleType(), float, 0.0),
    ("eth.src", StringType(), str, ""),
    ("eth.dst", StringType(), str, ""),
    ("arp.opcode", IntegerType(), int, 0),
    ("arp.src.hw_mac", StringType(), str, ""),
    ("arp.src.proto_ipv4", StringType(), str, ""),
    ("arp.dst.hw_mac", StringType(), str, ""),
    ("arp.dst.proto_ipv4", StringType(), str, ""),
    ("arp.isgratuitous", StringType(), str, ""),
]

def get_arp_schema() -> StructType:
    return StructType([
        StructField(name, spark_type, True)
        for name, spark_type, _, _ in ARP_FIELD_METADATA
    ])