"""Gold Layer: Business analytics, dimensional aggregates, and Delta Z-Order optimization."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
)
from pyspark.sql.functions import (
    round as spark_round,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)


class GoldAggregation:
    """Aggregates Silver data into high-value Gold Lakehouse business views."""

    def __init__(
        self,
        spark: SparkSession,
        gold_dir: Path | str,
        silver_dir: Path | str,
        bronze_dir: Path | str,
    ) -> None:
        self.spark = spark
        self.gold_dir = Path(gold_dir)
        self.silver_dir = Path(silver_dir)
        self.bronze_dir = Path(bronze_dir)

        self.kpis_table_path = str(self.gold_dir / "daily_city_kpis")
        self.stations_table_path = str(self.gold_dir / "station_utilization")
        self.silver_trips_path = str(self.silver_dir / "trips_enriched")
        self.bronze_stations_path = str(self.bronze_dir / "stations")

    def build_daily_city_kpis(self, silver_df: DataFrame | None = None) -> DataFrame:
        """Compute executive daily mobility KPIs per city and vehicle type.

        Metrics:
        - Total trips & unique active riders
        - Total revenue (EUR) and average fare per trip
        - Total distance traveled (km) and average trip duration (min)
        - Cumulative CO2 emissions prevented (kg)
        """
        source_df = (
            silver_df
            if silver_df is not None
            else self.spark.read.format("delta").load(self.silver_trips_path)
        )

        distinct_col = "user_id" if "user_id" in source_df.columns else "vehicle_id"

        gold_kpis = (
            source_df.groupBy("trip_date", "start_city", "vehicle_type")
            .agg(
                count("trip_id").alias("total_trips"),
                countDistinct(distinct_col).alias("active_units"),
                spark_round(spark_sum("distance_km"), 2).alias("total_distance_km"),
                spark_round(avg("duration_min"), 2).alias("avg_duration_min"),
                spark_round(spark_sum("fare_eur"), 2).alias("total_revenue_eur"),
                spark_round(avg("fare_eur"), 2).alias("avg_fare_eur"),
                spark_round(spark_sum("co2_saved_kg"), 3).alias("total_co2_saved_kg"),
            )
            .withColumnRenamed("start_city", "city")
            .orderBy("trip_date", "city", "vehicle_type")
        )

        # Write to Gold Delta table with date partitioning
        (
            gold_kpis.write.format("delta")
            .mode("overwrite")
            .partitionBy("trip_date")
            .save(self.kpis_table_path)
        )

        # Optimize file layout with Z-Ordering for lightning-fast multi-dimensional queries
        try:
            self.spark.sql(
                f"OPTIMIZE delta.`{self.kpis_table_path}` ZORDER BY (city, vehicle_type)"
            )
        except Exception:
            # Local Spark environments without proprietary Databricks runtime Z-Order fallback gracefully
            pass

        return gold_kpis

    def build_station_utilization(
        self, silver_df: DataFrame | None = None
    ) -> DataFrame:
        """Calculate station-level departure/arrival balance and utilization rate."""
        source_df = (
            silver_df
            if silver_df is not None
            else self.spark.read.format("delta").load(self.silver_trips_path)
        )

        stations_df = self.spark.read.format("delta").load(self.bronze_stations_path)

        departures = (
            source_df.groupBy("start_station", "trip_date")
            .agg(count("trip_id").alias("departures_count"))
            .withColumnRenamed("start_station", "station_id")
        )

        arrivals = (
            source_df.groupBy("end_station", "trip_date")
            .agg(count("trip_id").alias("arrivals_count"))
            .withColumnRenamed("end_station", "station_id")
        )

        # Combine departures and arrivals per station and date
        station_traffic = (
            departures.join(arrivals, ["station_id", "trip_date"], how="full_outer")
            .fillna(0, subset=["departures_count", "arrivals_count"])
            .withColumn("net_flow", col("arrivals_count") - col("departures_count"))
            .withColumn(
                "total_activity", col("departures_count") + col("arrivals_count")
            )
        )

        # Join with station metadata and compute capacity pressure
        enriched_utilization = (
            station_traffic.join(stations_df, on="station_id", how="inner")
            .withColumn(
                "turnover_rate",
                spark_round(col("total_activity") / col("capacity"), 2),
            )
            .select(
                "trip_date",
                "station_id",
                "station_name",
                "city",
                "capacity",
                "departures_count",
                "arrivals_count",
                "net_flow",
                "turnover_rate",
            )
            .orderBy("trip_date", "city", col("turnover_rate").desc())
        )

        (
            enriched_utilization.write.format("delta")
            .mode("overwrite")
            .partitionBy("trip_date")
            .save(self.stations_table_path)
        )

        return enriched_utilization

    def get_daily_city_kpis(self) -> DataFrame:
        """Load the Gold daily KPIs Delta table."""
        return self.spark.read.format("delta").load(self.kpis_table_path)

    def get_station_utilization(self) -> DataFrame:
        """Load the Gold station utilization Delta table."""
        return self.spark.read.format("delta").load(self.stations_table_path)
