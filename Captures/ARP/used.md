```bash

 tshark -r Capture_From_Victim_Side_Using_Harbor_copy.pcapng  -T fields -e frame.time_epoch -e eth.src -e eth.dst   -e arp.opcode -e arp.src.hw_mac -e arp.src.proto_ipv4   -e arp.dst.hw_mac -e arp.dst.proto_ipv4 -E header=y -E separator=, > arp_traffic.csv

```

2nd


```bash

tshark -r <file> -Y "arp" -T fields \
  -e frame.time_epoch \
  -e eth.src -e eth.dst \
  -e arp.opcode -e arp.src.hw_mac -e arp.src.proto_ipv4 \
  -e arp.dst.hw_mac -e arp.dst.proto_ipv4 \
  -e arp.isgratuitous \
  -E header=y -E separator=, > arp_rich.csv

```