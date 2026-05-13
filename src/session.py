from pyspark.sql import SparkSession
from config import ARPConfig

def create_spark_session(
    app_name: str = "ARP-Anomaly-Detection", streaming: bool = True
) -> SparkSession:
    """Create optimized Spark session for ARP processing."""

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.streaming.metricsEnabled", "true")
    )

    if streaming:
        builder = builder.config(
            "spark.sql.streaming.forceDeleteTempCheckpointLocation", "true"
        ).config(
            "spark.sql.streaming.checkpointLocation",
            f"{ARPConfig.CHECKPOINT_BASE}/global",
        )

    builder = builder.config("spark.hadoop.fs.defaultFS", "file:///")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark