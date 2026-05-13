import os
from pyspark.sql import SparkSession, DataFrame
from config import ARPConfig

class OutputManager:
    """Manages output sinks for detections."""

    def __init__(self, spark: SparkSession, config: ARPConfig):
        self.spark = spark
        self.config = config

    def console_sink(self, df: DataFrame, query_name: str, output_mode: str = "update"):
        """Write to console for debugging."""
        return (
            df.writeStream.outputMode(output_mode)
            .format("console")
            .option("truncate", "false")
            .option("numRows", 20)
            .queryName(query_name)
            .start()
        )

    def file_sink(
        self, df: DataFrame, query_name: str, subdir: str, output_mode: str = "append"
    ):
        """Write to Parquet files for downstream analysis."""
        output_path = f"{self.config.OUTPUT_BASE}/{subdir}"
        os.makedirs(output_path, exist_ok=True)

        return (
            df.writeStream.outputMode(output_mode)
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", f"{self.config.CHECKPOINT_BASE}/{subdir}")
            .queryName(query_name)
            .start()
        )

    def memory_sink(self, df: DataFrame, query_name: str):
        """Keep in memory for interactive querying."""
        return (
            df.writeStream.outputMode("complete")
            .format("memory")
            .queryName(query_name)
            .start()
        )