# load/execute.py

import os
import sys
import psycopg2
from pyspark.sql import SparkSession

def find_driver_jar():
    """Finds the path to the PostgreSQL JDBC driver JAR file."""
    for file in os.listdir('.'):
        if file.startswith("postgresql-") and file.endswith(".jar"):
            return os.path.abspath(file)
    return None

def create_spark_session():
    """Creates a SparkSession configured with the PostgreSQL JDBC driver."""
    driver_path = find_driver_jar()
    if not driver_path:
        print("FATAL: Could not find the PostgreSQL JDBC driver JAR file.")
        sys.exit(1)
        
    print(f"Using JDBC Driver: {driver_path}")
    
    spark = SparkSession.builder \
        .appName("FifaETLLoad") \
        .config("spark.jars", driver_path) \
        .getOrCreate()
    return spark

def create_tables():
    """Connects to the native PostgreSQL and creates tables for the FIFA data."""
    conn = None
    cur = None
    
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="fifadb",
            user="postgres",
            password="postgres"
        )
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS master_player_table;")
        cur.execute("DROP TABLE IF EXISTS player_metadata;")
        cur.execute("DROP TABLE IF EXISTS club_metadata;")
        
        # Schemas that EXACTLY MATCH the output of the transform script
        queries = [
            """
            CREATE TABLE master_player_table (
                full_name VARCHAR(255) PRIMARY KEY,
                short_name VARCHAR(100),
                age INTEGER,
                nationality VARCHAR(100),
                overall INTEGER,
                potential INTEGER,
                club_name VARCHAR(100),
                value_eur DOUBLE PRECISION,
                wage_eur DOUBLE PRECISION,
                preferred_foot VARCHAR(10)
            );
            """,
            """
            CREATE TABLE player_metadata (
                full_name VARCHAR(255) PRIMARY KEY,
                short_name VARCHAR(100),
                player_positions VARCHAR(100),
                age INTEGER,
                nationality VARCHAR(100),
                preferred_foot VARCHAR(10)
            );
            """,
            """
            CREATE TABLE club_metadata (
                club_name VARCHAR(100) PRIMARY KEY
            );
            """
        ]

        for query in queries:
            cur.execute(query)

        conn.commit()
        print("Tables dropped and recreated successfully.")
    except Exception as e:
        print(f"Error during table creation: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def load_data(spark, transformed_dir):
    """Loads transformed data from Parquet files into the native PostgreSQL tables."""
    jdbc_url = "jdbc:postgresql://localhost:5432/fifadb"
    connection_props = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    table_mapping = {
        "master_player_table": "stage2/master_player_table",
        "player_metadata": "stage3/player_metadata",
        "club_metadata": "stage3/club_metadata"
    }

    for table_name, folder_name in table_mapping.items():
        path = os.path.join(transformed_dir, folder_name)
        if os.path.isdir(path):
            print(f"Reading data from {path}...")
            df = spark.read.parquet(path)
            
            print(f"Appending data into table: {table_name}")
            df.write.jdbc(
                url=jdbc_url,
                table=table_name,
                mode="append",
                properties=connection_props
            )
        else:
            print(f"Warning: Data path not found, skipping: {path}")

# Main execution block
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} /path/to/transformed_data")
        sys.exit(1)

    transformed_path = sys.argv[1]

    spark = create_spark_session()
    create_tables()
    load_data(spark, transformed_path)
    
    print("ETL Load process completed successfully.")