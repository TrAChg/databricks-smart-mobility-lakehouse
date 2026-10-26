"""Utility script to quickly inspect Parquet files and Delta tables in the terminal."""

import sys
from pathlib import Path

import pandas as pd


def inspect_parquet(target_path: str) -> None:
    p = Path(target_path)
    if not p.exists():
        print(f"Error: Path '{target_path}' does not exist.")
        return

    # If directory, find first parquet file or load folder
    if p.is_dir():
        parquet_files = list(p.rglob("*.parquet"))
        if not parquet_files:
            print(f"No .parquet files found under '{target_path}'")
            return
        print(f"Found {len(parquet_files)} parquet file(s) under '{p.name}/'")
        file_to_read = parquet_files[0]
        print(f"Inspecting sample file: {file_to_read.name}")
    else:
        file_to_read = p

    df = pd.read_parquet(file_to_read)
    print("\n" + "=" * 70)
    print(f"PARQUET FILE: {file_to_read.name}")
    print(f"Rows: {len(df)} | Columns: {len(df.columns)}")
    print("=" * 70)
    print("\n--- SCHEMA & TYPES ---")
    for col, dtype in df.dtypes.items():
        print(f"  - {col}: {dtype}")

    print("\n--- DATA PREVIEW (First 5 rows) ---")
    print(df.head(5).to_string())
    print("=" * 70)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        target = sys.argv[1]
    else:
        # Default sample
        target = "data/lakehouse/bronze/stations"
    inspect_parquet(target)
