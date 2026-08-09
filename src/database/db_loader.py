# cspell:ignore dotenv sqlalchemy psycopg2 pyarrow fastparquet pgAdmin chunksize
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv, find_dotenv


# 1. Load environment variables

load_dotenv(find_dotenv())

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "transit_db")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Local Parquet cache written by spark_transform.py
_script_dir        = os.path.dirname(__file__)
PROCESSED_DATA_DIR = os.path.join(_script_dir, "..", "..", "data", "processed", "vehicles")



# 2. Database helpers

def build_engine():
    """Creates and returns a SQLAlchemy engine for the configured PostgreSQL DB."""
    if not DB_PASSWORD:
        raise EnvironmentError(
            "DB_PASSWORD is not set. Add it to your .env file."
        )
    conn_str = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(conn_str)


def ensure_table(engine):
    """
    Creates the vehicle_positions table if it does not already exist.
    Using explicit DDL means the table structure is always predictable,
    regardless of what Pandas infers from the Parquet schema.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS vehicle_positions (
        id               SERIAL PRIMARY KEY,
        feed_timestamp   BIGINT,
        entity_id        TEXT,
        trip_id          TEXT,
        route_id         TEXT,
        start_time_raw   TEXT,
        start_date       TEXT,
        latitude         DOUBLE PRECISION,
        longitude        DOUBLE PRECISION,
        bearing          DOUBLE PRECISION,
        vehicle_id       TEXT,
        vehicle_label    TEXT,
        current_status   TEXT,
        vehicle_timestamp BIGINT,
        vehicle_datetime  TIMESTAMP,
        feed_datetime     TIMESTAMP,
        speed_kmh         DOUBLE PRECISION,
        ingested_at       TIMESTAMP DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("  Table 'vehicle_positions' verified/created.")



# 3. Main loader

def load_to_postgres():
    """
    Reads the locally cached Parquet files produced by spark_transform.py
    and appends all records into the vehicle_positions PostgreSQL table.
    """
    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    engine = build_engine()

    # Verify connection before doing any work
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("  Connection OK.")

    ensure_table(engine)

    # Read all partitioned Parquet files from the local cache
    print(f"Reading Parquet files from: {PROCESSED_DATA_DIR}")
    df = pd.read_parquet(PROCESSED_DATA_DIR)
    print(f"  Loaded {len(df):,} records from Parquet.")

    # Push to PostgreSQL in one batch
    print(f"Pushing {len(df):,} records to 'vehicle_positions'...")
    df.to_sql(
        name="vehicle_positions",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",   # sends rows in batches for better performance
        chunksize=500,
    )
    print(f" Successfully loaded {len(df):,} transit records into PostgreSQL.")


if __name__ == "__main__":
    load_to_postgres()
