"""Silver Layer: Cleansed, enriched, and validated Delta Lake tables with CDC merge."""

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    to_timestamp,
    unix_timestamp,
    when,
)
from pyspark.sql.functions import (
    round as spark_round,
)


class SilverTransformation:
    """Cleanses, enriches, and merges Bronze data into the Silver Delta table."""

    def __init__(
        self,
        spark: SparkSession,
        silver_dir: Path | str,
        bronze_dir: Path | str,
    ) -> None:
        self.spark = spark
        self.silver_dir = Path(silver_dir)
        self.bronze_dir = Path(bronze_dir)
        self.trips_table_path = str(self.silver_dir / "trips_enriched")
        self.bronze_trips_path = str(self.bronze_dir / "trips")
        self.bronze_stations_path = str(self.bronze_dir / "stations")

    def transform_bronze_to_silver(self, bronze_df: DataFrame) -> DataFrame:
        """Apply data cleansing, quality rules, and carbon-savings feature engineering."""
        # 1. Cast timestamps and derive duration in minutes
        cleaned = (
            bronze_df.withColumn("start_ts", to_timestamp(col("start_time")))
            .withColumn("end_ts", to_timestamp(col("end_time")))
            .withColumn("trip_date", to_date(col("start_ts")))
            .withColumn(
                "duration_min",
                spark_round(
                    (unix_timestamp("end_ts") - unix_timestamp("start_ts")) / 60.0,
                    2,
                ),
            )
        )

        # 2. Quality filter: discard invalid/negative trips
        valid_trips = cleaned.filter(
            (col("distance_km") > 0.0)
            & (col("duration_min") > 0.0)
            & (col("duration_min") < 720.0)  # max 12 hours
            & (col("trip_id").isNotNull())
        )

        # 3. Deduplicate latest records per trip_id within the batch
        deduped = valid_trips.dropDuplicates(["trip_id"])

        # 4. Feature Engineering: Carbon Offset (CO2 savings vs combustion vehicles)
        # Average car = 150g CO2/km; ebike = 20g (saving 130g); escooter = 25g (saving 125g); EV = 60g (saving 90g)
        enriched = deduped.withColumn(
            "co2_saved_kg",
            spark_round(
                when(col("vehicle_type") == "ebike", col("distance_km") * 0.130)
                .when(col("vehicle_type") == "escooter", col("distance_km") * 0.125)
                .when(col("vehicle_type") == "ev_car", col("distance_km") * 0.090)
                .otherwise(col("distance_km") * 0.100),
                3,
            ),
        ).withColumn(
            "avg_speed_kmh",
            spark_round((col("distance_km") / (col("duration_min") / 60.0)), 2),
        )

        # 5. Dimensional Join: Enrich with station and city metadata
        stations_df = self.spark.read.format("delta").load(self.bronze_stations_path)

        enriched_with_city = (
            enriched.join(
                stations_df.select(
                    col("station_id").alias("s_id"),
                    col("city").alias("start_city"),
                    col("station_name").alias("start_station_name"),
                ),
                col("start_station") == col("s_id"),
                how="left",
            )
            .drop("s_id")
            .join(
                stations_df.select(
                    col("station_id").alias("e_id"),
                    col("city").alias("end_city"),
                    col("station_name").alias("end_station_name"),
                ),
                col("end_station") == col("e_id"),
                how="left",
            )
            .drop("e_id")
        )

        return enriched_with_city

    def merge_to_silver(self, incoming_df: DataFrame) -> None:
        """Perform Delta Lake ACID Merge (Upsert / CDC) into the Silver table."""
        if not DeltaTable.isDeltaTable(self.spark, self.trips_table_path):
            # First write: initialize the Delta table partitioned by trip_date
            (
                incoming_df.write.format("delta")
                .mode("overwrite")
                .partitionBy("trip_date")
                .save(self.trips_table_path)
            )
            return

        # Subsequent writes: ACID MERGE INTO (Update matched, insert new)
        target_table = DeltaTable.forPath(self.spark, self.trips_table_path)

        (
            target_table.alias("target")
            .merge(
                incoming_df.alias("source"),
                "target.trip_id = source.trip_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def get_silver_trips(self) -> DataFrame:
        """Load the enriched Silver Delta table."""
        return self.spark.read.format("delta").load(self.trips_table_path)
