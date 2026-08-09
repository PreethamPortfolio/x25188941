#!/bin/bash
# MASTER SCRIPT: Automates the entire ETL workflow
# Usage: bash run_pipeline.sh

set -e  # Exit immediately if any command fails

echo "=============================================="
echo " Starting Irish Transit Data Pipeline"
echo "=============================================="

echo ""
echo "Step 1: Ingesting Live NTA Data..."
python src/ingestion/transit_ingestion.py

echo ""
echo "Step 2: Processing via PySpark..."
python src/processing/spark_transform.py

echo ""
echo "Step 3: Loading Data to PostgreSQL..."
python src/database/db_loader.py

echo ""
echo "=============================================="
echo " Pipeline execution complete."
echo " Data is ready for EV Analytics."
echo "=============================================="
