"""Unit and integration tests for Bronze ingestion layer."""

from pyspark.sql import SparkSession

from src.lakehouse.bronze import BronzeIngestion
from src.lakehouse.config import LakehouseConfig


def test_ingest_stations_metadata(
    spark: SparkSession, temp_lakehouse_config: LakehouseConfig
) -> None:
    """Validate stations CSV ingestion into Delta Bronze table."""
    sample_csv = temp_lakehouse_config.raw_dir / "stations.csv"
    sample_csv.write_text(
        "station_id,station_name,city,capacity,latitude,longitude\n"
        "ST-PAR-01,Gare de Lyon,Paris,40,48.8443,2.3730\n"
        "ST-PAR-02,Chatelet,Paris,50,48.8584,2.3470\n",
        encoding="utf-8",
    )

    bronze = BronzeIngestion(spark=spark, bronze_dir=temp_lakehouse_config.bronze_dir)
    df = bronze.ingest_stations_metadata(sample_csv)

    assert df.count() == 2
    assert "_ingest_timestamp" in df.columns
    assert "_source_file" in df.columns

    # Verify Delta table persistence
    persisted_df = spark.read.format("delta").load(bronze.stations_table_path)
    assert persisted_df.count() == 2


def test_ingest_trips_batch(
    spark: SparkSession, temp_lakehouse_config: LakehouseConfig
) -> None:
    """Validate trips JSON ingestion with batch audit metadata."""
    sample_json = temp_lakehouse_config.raw_dir / "trips.json"
    sample_json.write_text(
        """[
            {"trip_id": "TRP-001", "user_id": "USR-1", "vehicle_type": "ebike", "start_time": "2026-01-01T08:00:00Z", "end_time": "2026-01-01T08:15:00Z", "start_station": "ST-01", "end_station": "ST-02", "distance_km": 3.5, "fare_eur": 2.50, "status": "completed"}
        ]""",
        encoding="utf-8",
    )

    bronze = BronzeIngestion(spark=spark, bronze_dir=temp_lakehouse_config.bronze_dir)
    df = bronze.ingest_trips_batch(sample_json, batch_id="test_batch_1")

    assert df.count() == 1
    assert "_batch_id" in df.columns
    first_row = df.select("_batch_id").first()
    assert first_row is not None
    assert first_row[0] == "test_batch_1"

    # Delta table check
    bronze_trips = bronze.get_bronze_trips()
    assert bronze_trips.count() == 1
