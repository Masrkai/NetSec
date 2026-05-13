# `schema.py` — ARP Packet Schema

## Overview

`schema.py` defines the Spark schema for raw ARP packet data exported from **Wireshark** or **tshark** as CSV. It is consumed by `ARPDataSource` in both batch and streaming reads to enforce typed parsing at ingestion time.

---

## Function: `get_arp_schema`

```python
def get_arp_schema() -> StructType:
```

Returns a `StructType` that maps directly to the column names produced by tshark's CSV export format.

### Schema Fields

| Column Name | Spark Type | Nullable | Description |
|---|---|---|---|
| `frame.time_epoch` | `DoubleType` | Yes | Unix timestamp of the captured packet (seconds with decimals) |
| `eth.src` | `StringType` | Yes | Source Ethernet (MAC) address |
| `eth.dst` | `StringType` | Yes | Destination Ethernet (MAC) address |
| `arp.opcode` | `IntegerType` | Yes | ARP operation code (`1` = request, `2` = reply) |
| `arp.src.hw_mac` | `StringType` | Yes | Sender MAC address in ARP payload |
| `arp.src.proto_ipv4` | `StringType` | Yes | Sender IP address in ARP payload |
| `arp.dst.hw_mac` | `StringType` | Yes | Target MAC address in ARP payload |
| `arp.dst.proto_ipv4` | `StringType` | Yes | Target IP address in ARP payload |
| `arp.isgratuitous` | `StringType` | Yes | Whether the packet is gratuitous (raw string from tshark: `"True"`, `"1"`, etc.) |

> **Note:** All fields are nullable (`True`) to accommodate `PERMISSIVE` parsing mode, which tolerates malformed or partial rows without failing the entire read.

---

## Usage

```python
from schema import get_arp_schema

schema = get_arp_schema()
df = spark.read.schema(schema).csv("./Captures/CSV")
```

---

## Design Notes

- The column names use dot notation (e.g., `frame.time_epoch`) to match tshark's default field naming — Spark handles these as string identifiers when backtick-quoted in `selectExpr`.
- `arp.isgratuitous` is kept as `StringType` because tshark may export it as `"True"`, `"1"`, `"yes"`, etc. The `enrichment.py` layer normalizes this into a proper boolean.
- `arp.opcode` is parsed as `IntegerType` directly; the enrichment layer then derives `is_request` and `is_reply` boolean flags from it.