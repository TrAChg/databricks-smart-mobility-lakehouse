@echo off
REM Batch runner for Databricks Smart Mobility Lakehouse Pipeline
cd /d "%~dp0"

IF NOT EXIST ".venv\Scripts\python.exe" (
    echo Virtual environment not found. Setting up with uv...
    uv sync
)

echo Executing Medallion Lakehouse Pipeline with .venv...
.venv\Scripts\python.exe -m src.lakehouse.pipeline
pause
