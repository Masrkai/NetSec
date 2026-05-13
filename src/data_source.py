import os
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col

from config import ARPConfig
from schema import get_arp_schema

class ARPDataSource:
    """Abstraction for ARP data sources."""

    def __init__(self, spark: SparkSession, config: ARPConfig):
        self.spark = spark
        self.config = config

    def read_csv_stream(self, csv_dir: Optional[str] = None) -> DataFrame:
        """Read ARP data from CSV files in streaming mode."""
        csv_path = csv_dir or self.config.CSV_INPUT_DIR
        abs_path = os.path.abspath(csv_path)
        if not os.path.isdir(abs_path):
            raise FileNotFoundError(f"CSV directory not found: {abs_path}")

        file_url = f"file://{abs_path}"
        print(f"[INFO] Streaming CSV source: {file_url}")

        return (
            self.spark.readStream.option("header", "true")
            .option("maxFilesPerTrigger", self.config.MAX_FILES_PER_TRIGGER)
            .option("pathGlobFilter", "*.csv")
            .option("recursiveFileLookup", "false")
            .option("ignoreLeadingWhiteSpace", "true")
            .option("ignoreTrailingWhiteSpace", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(get_arp_schema())
            .csv(file_url)
        )

    def read_csv_batch(self, csv_dir: Optional[str] = None) -> DataFrame:
        """Read ARP data from CSV files in batch mode."""
        csv_path = csv_dir or self.config.CSV_INPUT_DIR
        abs_path = os.path.abspath(csv_path)
        print(f"[INFO] Batch CSV source: {abs_path}")

        return (
            self.spark.read.option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(get_arp_schema())
            .csv(abs_path)
        )

    def read_kafka_stream(
        self, topic: Optional[str] = None, bootstrap_servers: Optional[str] = None
    ) -> DataFrame:
        """FUTURE: Read ARP data from Kafka topic in real-time."""
        topic = topic or self.config.KAFKA_TOPIC_ARP
        servers = bootstrap_servers or self.config.KAFKA_BOOTSTRAP_SERVERS
        print(f"[INFO] Kafka source: {servers}, topic: {topic}")

        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", servers)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
            .select(
                from_json(col("value").cast("string"), get_arp_schema()).alias("data")
            )
            .select("data.*")
        )