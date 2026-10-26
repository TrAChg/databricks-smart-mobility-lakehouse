"""Bronze Layer: Raw data ingestion into Delta Lake with audit metadata."""

import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit


class BronzeIngestion:
    """Ingests raw multi-source streams and batches into the Delta Lake Bronze layer."""

    def __init__(self, spark: SparkSession, bronze_dir: Path | str) -> None:
        self.spark = spark
        self.bronze_dir = Path(bronze_dir)
        self.trips_table_path = str(self.bronze_dir / "trips")
        self.stations_table_path = str(self.bronze_dir / "stations")

    def ingest_trips_batch(
        self,
        raw_json_path: Path | str,
        batch_id: str | None = None,
    ) -> DataFrame:
        """Ingest a batch of raw trip event logs into the Bronze Delta table."""
        
        source_str = str(raw_json_path)
        cur_batch = batch_id or f"batch_{int(time.time())}"

        # 1. Read raw JSON schema
        raw_df = self.spark.read.option("multiline", "true").json(source_str)

        # 2. Append technical audit columns (Raw Data Fidelity)
        bronze_df = (
            raw_df.withColumn("_ingest_timestamp", current_timestamp())
            .withColumn("_source_file", input_file_name())
            .withColumn("_batch_id", lit(cur_batch))
        )

        # 3. Write append-only to Delta Bronze table
        (
            bronze_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(self.trips_table_path)
        )

        return bronze_df

    def ingest_stations_metadata(self, raw_csv_path: Path | str) -> DataFrame:
        """Ingest station reference data into the Bronze Delta table."""
        source_str = str(raw_csv_path)

        stations_df = (
            self.spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(source_str)
            .withColumn("_ingest_timestamp", current_timestamp())
            .withColumn("_source_file", input_file_name())
        )

        (
            stations_df.write.format("delta")
            .mode("overwrite")
            .save(self.stations_table_path)
        )

        return stations_df

    def get_bronze_trips(self) -> DataFrame:
        """Load the accumulated Bronze trips Delta table."""
        return self.spark.read.format("delta").load(self.trips_table_path)
