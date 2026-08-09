Project: Distributed Processing of Irish Transit Data for Scalable EV Charging
Author: Preetham Nachimuttu (x25188941)
Course: MSc in Data Analytics, National College of Ireland (NCI)

------------------------------------------------------------------------
1. PROJECT OVERVIEW
------------------------------------------------------------------------
This project is an end-to-end data engineering pipeline designed to pull 
live public transit data across Ireland to map out vehicle density. By 
analyzing where traffic is highly concentrated, the goal is to determine 
the optimal, scalable locations for EV charging infrastructure. 

This is a solo academic project executed entirely from raw data ingestion 
to database loading and analytics.

------------------------------------------------------------------------
2. TECH STACK
------------------------------------------------------------------------
* Data Source: National Transport Authority (NTA) GTFS-Realtime API
* Processing: Python, PySpark (Java 17)
* Cloud Storage: Microsoft Azure Data Lake Gen2
* Database: PostgreSQL
* Analytics: Pandas & SQL

------------------------------------------------------------------------
3. REPOSITORY STRUCTURE
------------------------------------------------------------------------
/configs        -> Configuration files for APIs, Spark, and databases.
/data           -> Local caching for raw/processed Parquet files and CSVs.
/docs           -> Project reports, architecture diagrams, and coversheets.
/src            -> Main source code directory.
  /ingestion    -> Scripts to pull GTFS Protobuf data and upload to Azure.
  /processing   -> PySpark scripts to flatten, clean, and partition data.
  /database     -> Scripts to load cleaned Parquet files into PostgreSQL.
  /analytics    -> SQL/Pandas scripts to calculate EV hotspots.
/tests          -> Pipeline validation and testing scripts.
run_pipeline.sh -> Master shell script to automate the entire workflow.
requirements.txt-> Python dependencies required to run the project.

------------------------------------------------------------------------
4. HOW TO SET UP AND RUN
------------------------------------------------------------------------
Step 1: Environment Setup
Rename the '.env.example' file to '.env' in the root directory and fill 
in your specific credentials:
- NTA_API_KEY
- AZURE_STORAGE_CONNECTION_STRING
- DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

Step 2: Install Dependencies
Ensure Java 17 is installed for PySpark, then install the Python libraries:
> pip install -r requirements.txt

Step 3: Execute the Pipeline
The entire ETL workflow is automated. Run the master script from the root 
directory in your terminal:
> bash run_pipeline.sh

This script will automatically:
1. Ingest live vehicle positions from the NTA API to Azure.
2. Flatten the nested data using PySpark and save it as Parquet files.
3. Load the cleaned records into the local PostgreSQL database.

------------------------------------------------------------------------
5. CURRENT PIPELINE STATUS
------------------------------------------------------------------------
*  NTA API live connection established.
*  Raw JSON data successfully landing in Azure container (raw-transit).
*  PySpark transformation successfully flattening data and writing Parquet.
*  PostgreSQL integration complete (db_loader.py successfully appending records).
*  Final hotspot analytics and Tableau dashboard generation (In Progress).
