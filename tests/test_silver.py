"""Unit and integration tests for Silver layer: data quality, CO2 calculations, and CDC Merge."""

from pyspark.sql import SparkSession

from src.lakehouse.bronze import BronzeIngestion
from src.lakehouse.config import LakehouseConfig
from src.lakehouse.silver import SilverTransformation


def test_silver_cleansing_and_enrichment(
    spark: SparkSession, temp_lakehouse_config: LakehouseConfig
) -> None:
    """Verify quality gates, duration calculation, and CO2 offset."""
    bronze = BronzeIngestion(spark=spark, bronze_dir=temp_lakehouse_config.bronze_dir)
    silver = SilverTransformation(
        spark=spark,
        silver_dir=temp_lakehouse_config.silver_dir,
        bronze_dir=temp_lakehouse_config.bronze_dir,
    )

    # 1. Setup stations in Bronze
    sample_csv = temp_lakehouse_config.raw_dir / "stations.csv"
    sample_csv.write_text(
        "station_id,station_name,city,capacity,latitude,longitude\n"
        "ST-01,Gare de Lyon,Paris,40,48.8443,2.3730\n"
        "ST-02,Chatelet,Paris,50,48.8584,2.3470\n",
        encoding="utf-8",
    )
    bronze.ingest_stations_metadata(sample_csv)

    # 2. Setup trips with good and bad records
    sample_json = temp_lakehouse_config.raw_dir / "trips.json"
    sample_json.write_text(
        """[
            {"trip_id": "TRP-VALID-EBIKE", "user_id": "U1", "vehicle_type": "ebike", "start_time": "2026-01-01T10:00:00Z", "end_time": "2026-01-01T10:30:00Z", "start_station": "ST-01", "end_station": "ST-02", "distance_km": 10.0, "fare_eur": 4.5, "status": "completed"},
            {"trip_id": "TRP-INVALID-DIST", "user_id": "U2", "vehicle_type": "escooter", "start_time": "2026-01-01T10:00:00Z", "end_time": "2026-01-01T10:10:00Z", "start_station": "ST-01", "end_station": "ST-02", "distance_km": -1.0, "fare_eur": 2.0, "status": "completed"},
            {"trip_id": "TRP-INVALID-TIME", "user_id": "U3", "vehicle_type": "ev_car", "start_time": "2026-01-01T10:30:00Z", "end_time": "2026-01-01T10:00:00Z", "start_station": "ST-01", "end_station": "ST-02", "distance_km": 5.0, "fare_eur": 6.0, "status": "completed"}
        ]""",
        encoding="utf-8",
    )
    bronze_df = bronze.ingest_trips_batch(sample_json)

    # 3. Transform to Silver
    silver_df = silver.transform_bronze_to_silver(bronze_df)

    # The 2 invalid trips must be filtered out
    assert silver_df.count() == 1
    row = silver_df.first()
    assert row["trip_id"] == "TRP-VALID-EBIKE"
    assert row["duration_min"] == 30.0
    # 10.0 km * 0.130 kg CO2/km = 1.300 kg CO2 saved
    assert row["co2_saved_kg"] == 1.300
    assert row["start_city"] == "Paris"


def test_silver_acid_merge_cdc(
    spark: SparkSession, temp_lakehouse_config: LakehouseConfig
) -> None:
    """Verify Delta Lake MERGE INTO correctly updates matched rows and inserts new rows."""
    bronze = BronzeIngestion(spark=spark, bronze_dir=temp_lakehouse_config.bronze_dir)
    silver = SilverTransformation(
        spark=spark,
        silver_dir=temp_lakehouse_config.silver_dir,
        bronze_dir=temp_lakehouse_config.bronze_dir,
    )

    # Setup stations
    sample_csv = temp_lakehouse_config.raw_dir / "stations.csv"
    sample_csv.write_text(
        "station_id,station_name,city,capacity,latitude,longitude\n"
        "ST-01,Station A,Paris,40,48.8,2.3\n"
        "ST-02,Station B,Paris,50,48.9,2.4\n",
        encoding="utf-8",
    )
    bronze.ingest_stations_metadata(sample_csv)

    # Batch 1: Initial record
    b1_json = temp_lakehouse_config.raw_dir / "b1.json"
    b1_json.write_text(
        """[
            {"trip_id": "TRP-100", "user_id": "U1", "vehicle_type": "ebike", "start_time": "2026-01-01T08:00:00Z", "end_time": "2026-01-01T08:20:00Z", "start_station": "ST-01", "end_station": "ST-02", "distance_km": 4.0, "fare_eur": 2.0, "status": "ongoing"}
        ]""",
        encoding="utf-8",
    )
    b1_df = bronze.ingest_trips_batch(b1_json)
    silver_b1 = silver.transform_bronze_to_silver(b1_df)
    silver.merge_to_silver(silver_b1)

    initial_silver = silver.get_silver_trips()
    assert initial_silver.count() == 1
    assert initial_silver.first()["fare_eur"] == 2.0
    assert initial_silver.first()["status"] == "ongoing"

    # Batch 2: Update TRP-100 and insert TRP-200
    b2_json = temp_lakehouse_config.raw_dir / "b2.json"
    b2_json.write_text(
        """[
            {"trip_id": "TRP-100", "user_id": "U1", "vehicle_type": "ebike", "start_time": "2026-01-01T08:00:00Z", "end_time": "2026-01-01T08:35:00Z", "start_station": "ST-01", "end_station": "ST-02", "distance_km": 7.0, "fare_eur": 5.5, "status": "completed"},
            {"trip_id": "TRP-200", "user_id": "U2", "vehicle_type": "ev_car", "start_time": "2026-01-01T09:00:00Z", "end_time": "2026-01-01T09:30:00Z", "start_station": "ST-01", "end_station": "ST-02", "distance_km": 15.0, "fare_eur": 12.0, "status": "completed"}
        ]""",
        encoding="utf-8",
    )
    b2_df = bronze.ingest_trips_batch(b2_json)
    silver_b2 = silver.transform_bronze_to_silver(b2_df)
    silver.merge_to_silver(silver_b2)

    updated_silver = silver.get_silver_trips()
    # Total count should now be 2, NOT 3 (no duplicate TRP-100)
    assert updated_silver.count() == 2

    # Check that TRP-100 was updated
    updated_trp_100 = updated_silver.filter("trip_id = 'TRP-100'").first()
    assert updated_trp_100["fare_eur"] == 5.5
    assert updated_trp_100["status"] == "completed"
    assert updated_trp_100["distance_km"] == 7.0
