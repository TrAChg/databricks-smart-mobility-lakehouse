"""Configuration and SparkSession builder for Databricks Lakehouse."""

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession


@dataclass
class LakehouseConfig:
    """Paths and settings for the Medallion Lakehouse layers."""

    base_dir: Path
    raw_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path

    @classmethod
    def from_root(cls, root_dir: Path | str | None = None) -> "LakehouseConfig":
        """Instantiate configuration relative to project root."""
        if root_dir is None:
            # Default to repo root (2 levels up from src/lakehouse)
            root = Path(__file__).resolve().parent.parent.parent
        else:
            root = Path(root_dir)

        data_dir = root / "data"
        lakehouse_dir = data_dir / "lakehouse"

        return cls(
            base_dir=root,
            raw_dir=data_dir / "raw",
            bronze_dir=lakehouse_dir / "bronze",
            silver_dir=lakehouse_dir / "silver",
            gold_dir=lakehouse_dir / "gold",
        )


def get_spark_session(
    app_name: str = "SmartMobilityLakehouse",
    master: str = "local[*]",
) -> SparkSession:
    """Create or retrieve a SparkSession configured for Delta Lake.

    Enables:
    - Delta Lake ACID extensions and DeltaCatalog
    - Adaptive Query Execution (AQE)
    - Automatic schema evolution for Delta tables
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )

    try:
        from delta import configure_spark_with_delta_pip

        return configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        # Fallback to standard builder if delta python package pip helper is unavailable
        return builder.getOrCreate()
