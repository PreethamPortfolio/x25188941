Write-Host "Starting Distributed Processing Pipeline for Irish Transit Data..."

Write-Host "Step 1: Ingesting Live Data (10,000+ Record Loop)..."
python src/ingestion/ingestion_loop.py

Write-Host "Step 2: Processing via PySpark and Loading to PostgreSQL..."
python src/processing/spark_transform.py

Write-Host "Pipeline execution complete. Data is ready for Analytics."
