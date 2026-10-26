# PowerShell runner script for Databricks Smart Mobility Lakehouse Pipeline
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonExe = Join-Path $scriptDir ".venv\Scripts\python.exe"

if (-not (Test-Path $pythonExe)) {
    Write-Host "Virtual environment not found. Running uv sync..." -ForegroundColor Yellow
    uv sync
}

Write-Host "Executing Medallion Lakehouse Pipeline with .venv..." -ForegroundColor Cyan
& $pythonExe -m src.lakehouse.pipeline
