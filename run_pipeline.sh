#!/bin/bash
echo "Starting Distributed Processing Pipeline for Irish Transit Data..."

echo "Step 1: Ingesting Live Data (10,000+ Record Loop)..."
python src/ingestion/ingestion_loop.py

echo "Step 2: Processing via PySpark and Loading to PostgreSQL..."
python src/processing/spark_transform.py

echo "Pipeline execution complete. Data is ready for Analytics."


