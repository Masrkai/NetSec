# 1. Validate CSV schema matches code
head -2 CSV/arp_rich.csv | column -t -s,

# 2. Check if epoch timestamps are numeric (not quoted)
awk -F, 'NR==2 {print $1}' CSV/arp_rich.csv