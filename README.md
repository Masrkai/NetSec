# NetSec

This is a project done for the "Big Data" course to fullfil the requirement of using "Apache software" like hadoop spark and others in a useful manner
while this maybe not the most appropriate use of these tools and some others may be better candidates to work with

we are simply demonstrating a concept in real world with these tools

with that out of the way we need to bring up some other friends (projects) into the equation and let's separate them to 3 categories

- Home Made All in one
  - [Harbor](https://github.com/Masrkai/Harbor) a cli to carry ARP poisoning attack to bandwidth limit users at scale built by this repo creator @Masrkai

- External tools to carry the same concept as [Harbor](https://github.com/Masrkai/Harbor)
  - [Ettercap](https://github.com/Ettercap/ettercap) a cli tool to carry MITM attacks
  - [Nmap](https://github.com/nmap/nmap) a cli tool to scan networks

- Tools to capture and analyze "WTH is going on here"
  - [Wireshark](https://github.com/wireshark/wireshark) capturing the packets from the victim side (DW The victim studied was a vm)

We hope that the packets captures can provide some kind of pattern to alert or prevent certain attacks whenever possible the protocol we focused on is "ARP" present in the "IPV4" protocol created in 1970s and still present to this very day and used widely in many many networks

the captures emulates 2 conditions of which "ARP" is exploited once by [Harbor](https://github.com/Masrkai/Harbor) in which it ARP scans the networks then allow you to select a target or targets and limit their bandwidth using an MITM attack, another by [Nmap](https://github.com/nmap/nmap) for ARP network scanning then [Ettercao](https://github.com/Ettercap/ettercap) to do MITM attack exploiting ARP

this logic is very simple and other tools exist but these are the ones we are covering so:

    1. what is notable from these captures?
    2. what can we do with these data?

so to read this data we need some kind of convertor, `tshark` a utility from the wireshark package should be able to deal with this right ?

```bash

# Batch analysis (recommended for your current CSV)
python arp_detection.py --mode batch --csv-dir ./Captures/CSV

# Streaming (for continuous monitoring)
python arp_detection.py --mode stream --csv-dir ./Captures/CSV

# Future: Real-time from Kafka
python arp_detection.py --mode stream --kafka
```

what if we want to make a parameter to either read from that CSV or from live capture usign tshark how would we be able to implement that ?
