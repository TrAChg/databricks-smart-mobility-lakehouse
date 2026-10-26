import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

if sys.stdout.encoding != "utf-8":
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

from src.lakehouse.bronze import BronzeIngestion
from src.lakehouse.config import LakehouseConfig, get_spark_session
from src.lakehouse.gold import GoldAggregation
from src.lakehouse.silver import SilverTransformation


def run_lakehouse_pipeline(config: LakehouseConfig | None = None) -> None:
    """Execute the full Medallion Lakehouse lifecycle."""
    cfg = config or LakehouseConfig.from_root()
    spark: SparkSession = get_spark_session()

    print(f"Directory: {cfg.base_dir}")

    bronze = BronzeIngestion(spark=spark, bronze_dir=cfg.bronze_dir)
    silver = SilverTransformation(
        spark=spark, silver_dir=cfg.silver_dir, bronze_dir=cfg.bronze_dir
    )
    gold = GoldAggregation(
        spark=spark,
        gold_dir=cfg.gold_dir,
        silver_dir=cfg.silver_dir,
        bronze_dir=cfg.bronze_dir,
    )

    print(" Bronze: ")
    stations_csv = cfg.raw_dir / "stations.csv"
    bronze_stations_df = bronze.ingest_stations_metadata(stations_csv)
    print(f"  + {bronze_stations_df.count()} stations.")

    batch_1_file = cfg.raw_dir / "trips_batch_20260101.json"
    bronze_b1 = bronze.ingest_trips_batch(batch_1_file, batch_id="batch_20260101")
    print(f"  + Raw Batch 1: {bronze_b1.count()}")

    batch_2_file = cfg.raw_dir / "trips_batch_20260102.json"
    bronze_b2 = bronze.ingest_trips_batch(batch_2_file, batch_id="batch_20260102")
    print(f"  + Raw Batch 2: {bronze_b2.count()}")

    print("Silver: ")
    silver_b1 = silver.transform_bronze_to_silver(bronze_b1)
    silver.merge_to_silver(silver_b1)
    print(f"Batch 1 count: {silver.get_silver_trips().count()}")

    silver_b2 = silver.transform_bronze_to_silver(bronze_b2)
    silver.merge_to_silver(silver_b2)
    silver_all = silver.get_silver_trips()
    print(f"Silver Table count: {silver_all.count()}")

    # Display sample enriched records
    print("\n--- SAMPLE ENRICHED SILVER TRIPS ---")
    silver_all.select(
        "trip_id",
        "trip_date",
        "start_city",
        "vehicle_type",
        "distance_km",
        "duration_min",
        "co2_saved_kg",
        "fare_eur",
    ).show(5, truncate=False)

    print("Gold")
    gold_kpis = gold.build_daily_city_kpis(silver_all)
    gold_stations = gold.build_station_utilization(silver_all)

    print("\n---DAILY CITY MOBILITY KPIS ---")
    gold_kpis.show(10, truncate=False)

    print("\n---STATION UTILIZATION ---")
    gold_stations.show(10, truncate=False)

    print("\n Transaction Log...")
    silver_delta_table = DeltaTable.forPath(spark, silver.trips_table_path)
    print("--- TABLE HISTORY ---")
    silver_delta_table.history(3).select(
        "version", "timestamp", "operation", "operationMetrics"
    ).show(truncate=False)


if __name__ == "__main__":
    run_lakehouse_pipeline()
