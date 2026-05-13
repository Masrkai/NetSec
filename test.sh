# 1. Validate CSV schema matches code
head -2 Captures/CSV/arp_rich.csv | column -t -s,

# 2. Check if epoch timestamps are numeric (not quoted)
awk -F, 'NR==2 {print $1}' Captures/CSV/arp_rich.csv

# # 3. Test PySpark schema inference (debug only)
# python3 -c "
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()
# df = spark.read.option('header',True).csv('Captures/CSV/arp_rich.csv')
# df.printSchema()
# df.show(2, truncate=False)
# "

# # 4. Monitor streaming progress
# # In another terminal, watch checkpoint dirs:
# ls -la /tmp/spark-ckpt/*/