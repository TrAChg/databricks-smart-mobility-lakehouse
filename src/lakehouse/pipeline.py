import sys
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

if sys.stdout.encoding != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
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

    print("=" * 80)
    print(">>> STARTING DATABRICKS SMART MOBILITY LAKEHOUSE PIPELINE")
    print(f"Directory: {cfg.base_dir}")
    print("=" * 80)

    # 1. Initialize layer handlers
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

    # 2. Ingest Reference Stations to Bronze
    stations_csv = cfg.raw_dir / "stations.csv"
    print(f"\n[1/5] Ingesting Stations Reference Data -> Bronze: {stations_csv.name}")
    bronze_stations_df = bronze.ingest_stations_metadata(stations_csv)
    print(f"  + Ingested {bronze_stations_df.count()} stations.")

    # 3. Ingest Batch 1 to Bronze & Transform to Silver
    batch_1_file = cfg.raw_dir / "trips_batch_20260101.json"
    print(f"\n[2/5] Ingesting Batch 1 -> Bronze: {batch_1_file.name}")
    bronze_b1 = bronze.ingest_trips_batch(batch_1_file, batch_id="batch_20260101")
    print(f"  + Raw Batch 1 records: {bronze_b1.count()}")

    print("  -> Cleansing & Enrolling Batch 1 into Silver...")
    silver_b1 = silver.transform_bronze_to_silver(bronze_b1)
    silver.merge_to_silver(silver_b1)
    print(f"  + Silver Initial Table count: {silver.get_silver_trips().count()}")

    # 4. Ingest Batch 2 (CDC Upsert & New Records) -> Bronze & Merge into Silver
    batch_2_file = cfg.raw_dir / "trips_batch_20260102.json"
    print(f"\n[3/5] Ingesting Batch 2 (CDC Updates) -> Bronze: {batch_2_file.name}")
    bronze_b2 = bronze.ingest_trips_batch(batch_2_file, batch_id="batch_20260102")
    print(f"  + Raw Batch 2 records: {bronze_b2.count()}")

    print("  -> Performing Delta Lake ACID MERGE INTO (CDC Upsert) in Silver...")
    silver_b2 = silver.transform_bronze_to_silver(bronze_b2)
    silver.merge_to_silver(silver_b2)
    silver_all = silver.get_silver_trips()
    print(f"  + Silver Table count after CDC Merge: {silver_all.count()}")

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

    # 5. Build Gold Business Aggregations
    print("\n[4/5] Computing Gold Executive KPIs & Station Utilization...")
    gold_kpis = gold.build_daily_city_kpis(silver_all)
    gold_stations = gold.build_station_utilization(silver_all)

    print("\n--- GOLD: DAILY CITY MOBILITY KPIS ---")
    gold_kpis.show(10, truncate=False)

    print("\n--- GOLD: STATION UTILIZATION & PRESSURE ---")
    gold_stations.show(10, truncate=False)

    # 6. Audit & Delta Lake Transaction Log / Time Travel Demo
    print("\n[5/5] Auditing Delta Lake ACID Transaction Log...")
    silver_delta_table = DeltaTable.forPath(spark, silver.trips_table_path)
    print("--- DELTA TABLE HISTORY (First 3 Versions) ---")
    silver_delta_table.history(3).select(
        "version", "timestamp", "operation", "operationMetrics"
    ).show(truncate=False)

    print("\n" + "=" * 80)
    print(">>> DATABRICKS SMART MOBILITY LAKEHOUSE PIPELINE COMPLETED SUCCESSFULLY!")
    print("=" * 80)


if __name__ == "__main__":
    run_lakehouse_pipeline()
