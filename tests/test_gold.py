"""Unit and integration tests for Gold layer business aggregations."""

from pyspark.sql import SparkSession

from src.lakehouse.bronze import BronzeIngestion
from src.lakehouse.config import LakehouseConfig
from src.lakehouse.gold import GoldAggregation
from src.lakehouse.silver import SilverTransformation


def test_gold_daily_city_kpis_and_station_utilization(
    spark: SparkSession, temp_lakehouse_config: LakehouseConfig
) -> None:
    """Validate executive KPIs calculation and station net flow."""
    bronze = BronzeIngestion(spark=spark, bronze_dir=temp_lakehouse_config.bronze_dir)
    silver = SilverTransformation(
        spark=spark,
        silver_dir=temp_lakehouse_config.silver_dir,
        bronze_dir=temp_lakehouse_config.bronze_dir,
    )
    gold = GoldAggregation(
        spark=spark,
        gold_dir=temp_lakehouse_config.gold_dir,
        silver_dir=temp_lakehouse_config.silver_dir,
        bronze_dir=temp_lakehouse_config.bronze_dir,
    )

    # 1. Reference stations
    sample_csv = temp_lakehouse_config.raw_dir / "stations.csv"
    sample_csv.write_text(
        "station_id,station_name,city,capacity,latitude,longitude\n"
        "ST-01,Station A,Paris,20,48.8,2.3\n"
        "ST-02,Station B,Paris,20,48.9,2.4\n",
        encoding="utf-8",
    )
    bronze.ingest_stations_metadata(sample_csv)

    # 2. Trips: 2 trips for Paris e-bikes
    sample_json = temp_lakehouse_config.raw_dir / "trips.json"
    sample_json.write_text(
        """[
            {"trip_id": "T1", "user_id": "U1", "vehicle_type": "ebike", "start_time": "2026-01-01T08:00:00Z", "end_time": "2026-01-01T08:30:00Z", "start_station": "ST-01", "end_station": "ST-02", "distance_km": 10.0, "fare_eur": 5.0, "status": "completed"},
            {"trip_id": "T2", "user_id": "U2", "vehicle_type": "ebike", "start_time": "2026-01-01T09:00:00Z", "end_time": "2026-01-01T09:30:00Z", "start_station": "ST-01", "end_station": "ST-02", "distance_km": 10.0, "fare_eur": 5.0, "status": "completed"}
        ]""",
        encoding="utf-8",
    )
    bronze_df = bronze.ingest_trips_batch(sample_json)
    silver_df = silver.transform_bronze_to_silver(bronze_df)
    silver.merge_to_silver(silver_df)

    # 3. Gold Daily KPIs
    kpis_df = gold.build_daily_city_kpis(silver.get_silver_trips())
    assert kpis_df.count() == 1
    row = kpis_df.first()
    assert row["city"] == "Paris"
    assert row["vehicle_type"] == "ebike"
    assert row["total_trips"] == 2
    assert row["active_units"] == 2
    assert row["total_revenue_eur"] == 10.0
    assert row["total_distance_km"] == 20.0

    # 4. Gold Station Utilization
    station_df = gold.build_station_utilization(silver.get_silver_trips())
    # ST-01 had 2 departures, 0 arrivals -> net flow -2
    st1 = station_df.filter("station_id = 'ST-01'").first()
    assert st1["departures_count"] == 2
    assert st1["arrivals_count"] == 0
    assert st1["net_flow"] == -2

    # ST-02 had 0 departures, 2 arrivals -> net flow +2
    st2 = station_df.filter("station_id = 'ST-02'").first()
    assert st2["departures_count"] == 0
    assert st2["arrivals_count"] == 2
    assert st2["net_flow"] == 2
