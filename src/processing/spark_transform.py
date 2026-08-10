import os
import yaml
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, round, count, max as spark_max, lit, hour, avg
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType


# 1. Loaded the environment & config

load_dotenv()

_config_path = os.path.join(
    os.path.dirname(__file__), "..", "..", "configs", "spark_config.yaml"
)
with open(_config_path, "r") as _f:
    _cfg = yaml.safe_load(_f)

AZURE_CONN_STR   = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
RAW_CONTAINER    = "raw-transit"
PROC_CONTAINER   = "processed-transit"


# 2. Build Spark session

def get_spark(account_name: str, account_key: str) -> SparkSession:
    """Creates and returns a configured SparkSession with Azure credentials."""
    spark_cfg = _cfg.get("spark", {})
    return (
        SparkSession.builder
        .appName(spark_cfg.get("app_name", "TransitGridPipeline"))
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6,org.postgresql:postgresql:42.7.1")
        .config("spark.executor.memory", spark_cfg.get("executor_memory", "2g"))
        .config("spark.driver.memory",   spark_cfg.get("driver_memory",   "2g"))
        # Inject Azure storage account credentials so hadoop-azure can authenticate
        .config(f"spark.hadoop.fs.azure.account.key.{account_name}.blob.core.windows.net", account_key)
        # Use commit algorithm v2: writes tasks directly to destination, avoids
        # the copy-then-rename that fails on wasbs:// (StorageException)
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )


# 3. Read raw JSON blobs from Azure

def read_raw_json(spark: SparkSession, path: str):
    """
    Reads all raw GTFS-RT JSON files from the Azure Blob path.

    Args:
        spark: Active SparkSession.
        path:  abfss:// or wasbs:// path pointing to the raw container.

    Returns:
        DataFrame with the raw nested JSON structure.
    """
    return spark.read.option("multiLine", True).json(path)



# 4. Flatten & clean the GTFS-RT nested structure

def _has_nested_field(schema, dot_path: str) -> bool:
    """
    Checks whether a dot-separated field path exists in a PySpark schema.
    Handles both StructType and ArrayType transparently.
    """
    from pyspark.sql.types import StructType, ArrayType
    current = schema
    for part in dot_path.split("."):
        if isinstance(current, ArrayType):
            current = current.elementType
        if not isinstance(current, StructType):
            return False
        names = {f.name for f in current.fields}
        if part not in names:
            return False
        current = current[part].dataType
    return True


def flatten_transit_data(raw_df):
    """
    Flattens the GTFS-RT FeedMessage structure, extracts key vehicle fields,
    and cleans timestamps. All optional GTFS-RT fields are guarded with
    schema introspection so the pipeline works with any feed variant.

    GTFS-RT optional fields handled gracefully (null if absent):
        position.speed, vehicle.label

    Returns:
        Flat DataFrame with one row per vehicle entity.
    """
    # Explode the entity array so we get one row per vehicle report
    exploded = raw_df.select(
        F.col("header.timestamp").alias("feed_timestamp"),
        F.explode("entity").alias("entity"),
    )

    # --- Guard every optional GTFS-RT field against missing schema fields ---
    schema = exploded.schema

    def _opt(path, dtype):
        """Return column cast to dtype, or null literal if path is absent in schema."""
        return (
            F.col(path).cast(dtype)
            if _has_nested_field(schema, path)
            else F.lit(None).cast(dtype)
        )

    def _opt_str(path):
        """Return string column, or null literal if path is absent in schema."""
        from pyspark.sql.types import StringType
        return (
            F.col(path)
            if _has_nested_field(schema, path)
            else F.lit(None).cast(StringType())
        )

    # Flatten nested structs into individual columns
    flat = exploded.select(
        F.col("feed_timestamp").cast(LongType()),
        F.col("entity.id").alias("entity_id"),

        # Trip info (all optional in GTFS-RT spec)
        _opt_str("entity.vehicle.trip.tripId").alias("trip_id"),
        _opt_str("entity.vehicle.trip.routeId").alias("route_id"),
        _opt_str("entity.vehicle.trip.startTime").alias("start_time_raw"),
        _opt_str("entity.vehicle.trip.startDate").alias("start_date"),

        # Position info
        _opt("entity.vehicle.position.latitude",  DoubleType()).alias("latitude"),
        _opt("entity.vehicle.position.longitude", DoubleType()).alias("longitude"),
        _opt("entity.vehicle.position.bearing",   DoubleType()).alias("bearing"),
        _opt("entity.vehicle.position.speed",     DoubleType()).alias("speed_mps"),

        # Vehicle metadata
        _opt_str("entity.vehicle.vehicle.id").alias("vehicle_id"),
        _opt_str("entity.vehicle.vehicle.label").alias("vehicle_label"),
        _opt_str("entity.vehicle.currentStatus").alias("current_status"),
        _opt("entity.vehicle.timestamp", LongType()).alias("vehicle_timestamp"),
    )

    # Derive human-readable timestamps from Unix epoch seconds
    clean = flat.withColumn(
        "vehicle_datetime",
        F.to_timestamp(F.col("vehicle_timestamp"))
    ).withColumn(
        "feed_datetime",
        F.to_timestamp(F.col("feed_timestamp"))
    ).withColumn(
        # Convert speed m/s → km/h (null if feed omits speed)
        "speed_kmh",
        F.round(F.col("speed_mps") * 3.6, 2)
    ).drop("speed_mps")

    return clean



# 5. Analytics (Trust & Soft Labels)

def process_distributed_analytics(df):
    """Executes Data Trust and Soft Labeling within the Spark DAG."""
    print("Calculating Distributed Trust Scores...")
    
    # --- 1. DATA TRUST PIPELINE ---
    # Base score of 1.0
    df = df.withColumn("trust_score", lit(1.0))
    
    # Rule A: Zeroed coordinates penalty (-0.3)
    df = df.withColumn("trust_score", 
                       when((col("latitude") == 0) | (col("longitude") == 0), col("trust_score") - 0.3)
                       .otherwise(col("trust_score")))
                       
    # Rule B: Missing or negative speed penalty (-0.2)
    df = df.withColumn("trust_score", 
                       when(col("speed_kmh").isNull() | (col("speed_kmh") < 0), col("trust_score") - 0.2)
                       .otherwise(col("trust_score")))
                       
    # Rule C: Missing bearing penalty (-0.1)
    df = df.withColumn("trust_score", 
                       when(col("bearing").isNull(), col("trust_score") - 0.1)
                       .otherwise(col("trust_score")))

    # --- 2. SOFT LABEL PIPELINE ---
    print("Calculating Distributed Soft Labels (EV Viability)...")
    
    # Extract hour of day for the time slider
    df = df.withColumn("hour_of_day", hour(col("vehicle_datetime")))
    
    # Create ~110m geographic bins
    df = df.withColumn("cluster_lat", round(col("latitude"), 3))
    df = df.withColumn("cluster_lon", round(col("longitude"), 3))
    
    # Calculate vehicle density per cluster AND hour using a Window partition
    cluster_window = Window.partitionBy("cluster_lat", "cluster_lon", "hour_of_day")
    df = df.withColumn("density", count("vehicle_id").over(cluster_window))
    
    # Calculate max density globally to normalize the score (0.0 to 1.0)
    global_window = Window.partitionBy(lit(1))
    df = df.withColumn("max_density", spark_max("density").over(global_window))
    
    df = df.withColumn("normalized_density", 
                       when(col("max_density") > 0, col("density") / col("max_density"))
                       .otherwise(0))
                       
    # Calculate Final Viability Probability (70% Density, 30% Trust)
    df = df.withColumn("ev_viability_prob", (col("normalized_density") * 0.7) + (col("trust_score") * 0.3))
    
    # Cap the probability at 1.0 (100%)
    df = df.withColumn("ev_viability_prob", 
                       when(col("ev_viability_prob") > 1.0, 1.0)
                       .otherwise(col("ev_viability_prob")))
                       
    # Clean up temporary calculation columns
    df = df.drop("max_density")
    
    return df

def process_traffic_bottlenecks(df):
    """Calculates average speeds and vehicle counts per geographic cluster."""
    print("Calculating Traffic Bottlenecks...")
    return df.groupBy("cluster_lat", "cluster_lon").agg(
        round(avg("speed_kmh"), 2).alias("avg_speed_kmh"),
        count("vehicle_id").alias("vehicle_count")
    )

def process_fleet_density(df):
    """Calculates total active transit vehicles grouped by hour of the day."""
    print("Calculating Hourly Fleet Density...")
    return df.groupBy("hour_of_day").agg(
        count("vehicle_id").alias("total_active_vehicles")
    )


# 6. Write transformed data — local Parquet then upload to Azure

def write_processed_local(df, local_path: str, mode: str = "append"):
    """
    Writes the transformed DataFrame to a local Parquet directory,
    partitioned by start_date.

    wasbs:// write is avoided because the NativeAzureFileSystem commit
    protocol fails on Windows when cleaning up _temporary dirs.
    Use upload_to_azure() after this call to push files to Azure.

    Args:
        df:         Flat, cleaned DataFrame.
        local_path: Local filesystem output directory.
        mode:       Spark write mode ('append' or 'overwrite').
    """
    os.makedirs(local_path, exist_ok=True)
    (
        df.write
        .mode(mode)
        .partitionBy("start_date")
        .parquet(local_path)
    )
    print(f" Parquet written locally to: {local_path}")


def upload_to_azure(local_path: str, conn_str: str, container: str, prefix: str = "vehicles"):
    """
    Uploads all Parquet files from a local directory tree to Azure Blob Storage.

    Args:
        local_path: Root of the local Parquet output.
        conn_str:   Azure storage connection string.
        container:  Target container name.
        prefix:     Blob name prefix (virtual folder inside the container).
    """
    from azure.storage.blob import BlobServiceClient
    client = BlobServiceClient.from_connection_string(conn_str)
    container_client = client.get_container_client(container)

    uploaded = 0
    for root, _, files in os.walk(local_path):
        for fname in files:
            if not fname.endswith(".parquet") and fname != "_SUCCESS":
                continue
            full_path = os.path.join(root, fname)
            # Preserve the partition folder structure inside the blob prefix
            relative  = os.path.relpath(full_path, local_path).replace("\\", "/")
            blob_name = f"{prefix}/{relative}"
            with open(full_path, "rb") as f:
                container_client.upload_blob(blob_name, f, overwrite=True)
            uploaded += 1

    print(f" Uploaded {uploaded} file(s) to Azure container '{container}/{prefix}'")



def write_to_postgres(df, table_name, jdbc_url, user, password):
    """Helper function to append a PySpark DataFrame to a PostgreSQL table."""
    try:
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"ERROR writing to PostgreSQL table {table_name}: {e}")

# 6. Main ETL entrypoint

def run_transform(raw_path: str, local_out: str, conn_str: str,
                  account_name: str, account_key: str):
    """
    Full ETL pipeline:
        raw Azure JSON  ->  flatten / clean  ->  distributed analytics  ->  PostgreSQL.
    """
    spark = get_spark(account_name, account_key)
    print(f"Reading raw data from: {raw_path}")
    raw_df = read_raw_json(spark, raw_path)

    print("Flattening GTFS-RT vehicle records...")
    clean_df = flatten_transit_data(raw_df)
    clean_df.printSchema()

    record_count = clean_df.count()
    print(f" {record_count:,} vehicle records ready for processing.")

    # 1. Compute Base Hubs and Soft Labels
    analyzed_df = process_distributed_analytics(clean_df)
    
    # 2. Compute the new analytical tables
    bottlenecks_df = process_traffic_bottlenecks(analyzed_df)
    fleet_df = process_fleet_density(analyzed_df)
    
    # WRITE DIRECTLY TO POSTGRESQL
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "transit_db")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    print(f"Writing {analyzed_df.count()} analyzed records directly to PostgreSQL...")
    write_to_postgres(analyzed_df, "ev_viability_hubs", jdbc_url, DB_USER, DB_PASSWORD)
    
    print(f"Writing {bottlenecks_df.count()} bottleneck records to PostgreSQL...")
    write_to_postgres(bottlenecks_df, "traffic_bottlenecks", jdbc_url, DB_USER, DB_PASSWORD)
    
    print(f"Writing {fleet_df.count()} fleet density records to PostgreSQL...")
    write_to_postgres(fleet_df, "fleet_density_hourly", jdbc_url, DB_USER, DB_PASSWORD)
    
    print("SUCCESS: All analytical datasets securely loaded into PostgreSQL")
        
    spark.stop()


if __name__ == "__main__":
    # Parse AccountName and AccountKey from the connection string
    _account_name = None
    _account_key  = None
    if AZURE_CONN_STR:
        for part in AZURE_CONN_STR.split(";"):
            if part.startswith("AccountName="):
                _account_name = part.split("=", 1)[1]
            elif part.startswith("AccountKey="):
                _account_key = part.split("=", 1)[1]

    if not _account_name or not _account_key:
        raise EnvironmentError(
            "AZURE_STORAGE_CONNECTION_STRING is not set or missing AccountName/AccountKey."
        )

    # Read from Azure via Spark (wasbs://), write locally, then upload via SDK
    RAW_PATH  = f"wasbs://{RAW_CONTAINER}@{_account_name}.blob.core.windows.net/*.json"
    LOCAL_OUT = os.path.join(
        os.path.dirname(__file__), "..", "..", "data", "processed", "spark_analytics"
    )

    run_transform(RAW_PATH, LOCAL_OUT, AZURE_CONN_STR, _account_name, _account_key)
