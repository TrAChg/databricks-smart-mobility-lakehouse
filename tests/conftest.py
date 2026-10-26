"""Pytest test fixtures and SparkSession configuration for Delta Lake testing."""

import shutil
from pathlib import Path
from typing import Generator

import pytest
from pyspark.sql import SparkSession

from src.lakehouse.config import LakehouseConfig


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Provide a shared, lightweight SparkSession configured with Delta Lake."""
    builder = (
        SparkSession.builder.appName("LakehousePytestSession")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
    )

    try:
        from delta import configure_spark_with_delta_pip

        session = configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        session = builder.getOrCreate()

    yield session
    session.stop()


@pytest.fixture
def temp_lakehouse_config(tmp_path: Path) -> Generator[LakehouseConfig, None, None]:
    """Provide an isolated, temporary Lakehouse directory structure for each test."""
    raw_dir = tmp_path / "raw"
    lakehouse_dir = tmp_path / "lakehouse"
    bronze_dir = lakehouse_dir / "bronze"
    silver_dir = lakehouse_dir / "silver"
    gold_dir = lakehouse_dir / "gold"

    raw_dir.mkdir(parents=True, exist_ok=True)
    bronze_dir.mkdir(parents=True, exist_ok=True)
    silver_dir.mkdir(parents=True, exist_ok=True)
    gold_dir.mkdir(parents=True, exist_ok=True)

    config = LakehouseConfig(
        base_dir=tmp_path,
        raw_dir=raw_dir,
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
    )

    yield config

    # Clean up directory after test
    shutil.rmtree(tmp_path, ignore_errors=True)
