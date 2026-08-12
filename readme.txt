Project: Distributed Processing of Irish Transit Data for Scalable EV Charging
Author: Preetham Nachimuttu (x25188941)
Course: MSc in Data Analytics, National College of Ireland (NCI)

---------------------------------------------------------------------------
1. PROJECT OVERVIEW
---------------------------------------------------------------------------
This project is an end-to-end data engineering and analytics pipeline 
designed to ingest live public transit telemetry and power grid demand 
across Ireland. By engineering distributed spatial features and analyzing 
traffic bottlenecks against fleet density, the goal is to determine the 
optimal, scalable locations for Electric Vehicle (EV) charging 
infrastructure.
  This project executed entirely from raw API data 
ingestion to distributed processing, relational database loading, and 
executive business intelligence visualization.

---------------------------------------------------------------------------
2. TECH STACK
---------------------------------------------------------------------------
* Data Sources: National Transport Authority (NTA) GTFS-Realtime API & 
                EirGrid Smart Grid API
* Processing Engine: Python, Apache PySpark (running on Java 17)
* Cloud Storage: Microsoft Azure Data Lake Gen2 (Blob Storage)
* Database: PostgreSQL
* Visualization & Analytics: Microsoft Power BI
* Orchestration: Windows PowerShell

---------------------------------------------------------------------------
3. REPOSITORY STRUCTURE
---------------------------------------------------------------------------
/configs            -> Configuration files for APIs, Spark, and databases.
/data               -> Local caching for raw/processed Parquet/CSVs.
/docs               -> Power BI visualization.
/src                -> Main source code directory.
  /ingestion        -> Scripts (ingestion_loop.py) to pull live API data 
                       and upload to Azure.
  /processing       -> PySpark scripts (spark_transform.py) for distributed 
                       transformations.
/tests              -> Empty, Because it run full pipeline end-to-end manually.
.env.example        -> Template for environment variables and secrets.
requirements.txt    -> Python dependencies required to run the project.
run_pipeline.ps1    -> Master orchestrator script natively configured 
                       for Windows.

---------------------------------------------------------------------------
4. HOW TO SET UP AND RUN (CONFIGURATION MANUAL)
---------------------------------------------------------------------------
Step 1: System Prerequisites
Ensure the following are installed on your local Windows machine:
A. Python 3.9+
B. Java 17 (Required for local PySpark execution)
C. PostgreSQL (Running locally on default port 5432)

Step 2: Environment Configuration
1. Rename the '.env.example' file to '.env' in the root directory.
2. Fill in your specific secure credentials:
   - NTA_API_KEY
   - AZURE_STORAGE_CONNECTION_STRING
   - DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

Step 3: Python Virtual Environment Setup
Open your Windows PowerShell terminal in the root directory of the project 
and execute the following commands to isolate and install dependencies:

> python -m venv venv
> Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
> .\venv\Scripts\Activate.ps1
> pip install -r requirements.txt

Step 4: Execute the Pipeline
The entire ETL workflow is automated via a master PowerShell script. This 
script handles the multi-cycle API ingestion to Azure, triggers the 
PySpark cluster for heavy spatial transformations, and loads the engineered 
datasets directly into PostgreSQL.

Run the following command in your active PowerShell terminal:

> .\run_pipeline.ps1

Step 5: View Analytics
Once the pipeline execution terminal reads "SUCCESS: All analytical 
datasets securely loaded into PostgreSQL", open the 'transit_dashboard.pbix' 
file in Microsoft Power BI. Click 'Refresh' on the Home ribbon to pull 
the newly generated data from your local PostgreSQL instance into the 
visualization suite.

---------------------------------------------------------------------------
5. PIPELINE COMPLETION STATUS
---------------------------------------------------------------------------
* [COMPLETED] NTA and EirGrid API live connections established with 
  automated error-handling/rate-limiting.
* [COMPLETED] Raw JSON transit and grid data successfully landing in Azure 
  cloud containers.
* [COMPLETED] PySpark engine successfully processing 38,000+ records, 
  calculating 'ev_viability_prob' and bottleneck metrics.
* [COMPLETED] PostgreSQL integration fully operational (schema enforced, 
  tables populated).
* [COMPLETED] End-to-End Power BI dashboard finalized, featuring geospatial 
  mapping, fleet density clustering, and speed degradation curves.