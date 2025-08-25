# transform/execute.py

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def create_spark_session():
    """Creates and returns a SparkSession."""
    return SparkSession.builder.appName("FifaDataTransformation").getOrCreate()

def load_and_clean_players(spark, input_path, output_path):
    """
    Stage 1: Load the raw FIFA CSV, clean it, de-duplicate, and save the result.
    """
    print("--- Stage 1: Loading and Cleaning Player Data ---")
    
    players_df = spark.read.csv(input_path, header=True, inferSchema=True, escape='"')
    print(f"Successfully read raw data from {input_path}")

    required_columns = [
        "Known As", "Full Name", "Age", "Nationality", "Overall", "Potential", 
        "Club Name", "Value(in Euro)", "Wage(in Euro)",
        "Positions Played", "Preferred Foot"
    ]
            
    players_df = players_df.select(required_columns)
    players_df = players_df.dropna(subset=["Known As", "Value(in Euro)", "Club Name", "Full Name"])

    players_df = players_df \
        .withColumnRenamed("Known As", "short_name") \
        .withColumnRenamed("Full Name", "full_name") \
        .withColumnRenamed("Age", "age") \
        .withColumnRenamed("Nationality", "nationality") \
        .withColumnRenamed("Overall", "overall") \
        .withColumnRenamed("Potential", "potential") \
        .withColumnRenamed("Club Name", "club_name") \
        .withColumnRenamed("Value(in Euro)", "value_eur") \
        .withColumnRenamed("Wage(in Euro)", "wage_eur") \
        .withColumnRenamed("Positions Played", "player_positions") \
        .withColumnRenamed("Preferred Foot", "preferred_foot")
        
    # --- DE-DUPLICATION LOGIC ---
    # This ensures that 'full_name' is unique, which we need for our PRIMARY KEY.
    # If multiple players have the same name, we keep the one with the highest 'overall' rating.
    print("De-duplicating players based on full_name...")
    windowSpec  = Window.partitionBy("full_name").orderBy(F.col("overall").desc())
    
    players_df = players_df.withColumn("row_number", F.row_number().over(windowSpec)) \
                           .filter(F.col("row_number") == 1) \
                           .drop("row_number")
    # --- END OF DE-DUPLICATION LOGIC ---
        
    players_df.write.mode("overwrite").parquet(os.path.join(output_path, "stage1", "cleaned_players"))
    
    print(f"Stage 1 Complete. Cleaned player data saved. Total unique players: {players_df.count()}")
    return players_df

def create_master_table(players_df, output_path):
    """
    Stage 2: Create a master table.
    """
    print("--- Stage 2: Creating Master Table ---")

    # FIX: Add 'preferred_foot' to the list of selected columns
    master_df = players_df.select(
        "full_name", "short_name", "age", "nationality", "overall", "potential",
        "club_name", "value_eur", "wage_eur", "preferred_foot"  # <-- ADDED HERE
    )
    
    master_df.write.mode("overwrite").parquet(os.path.join(output_path, "stage2", "master_player_table"))
    
    print(f"Stage 2 Complete. Master table saved. Total records: {master_df.count()}")

def create_query_tables(players_df, output_path):
    """
    Stage 3: Create smaller, query-optimized "metadata" tables.
    """
    print("--- Stage 3: Creating Query-Optimized Tables ---")

    # Player Metadata Table
    player_metadata = players_df.select(
        "full_name", "short_name", "player_positions", "age", "nationality", "preferred_foot"
    ).distinct()
    player_metadata.write.mode("overwrite").parquet(os.path.join(output_path, "stage3", "player_metadata"))
    print("Saved player_metadata.")

    # Club Metadata Table
    club_metadata = players_df.select("club_name").distinct()
    club_metadata.write.mode("overwrite").parquet(os.path.join(output_path, "stage3", "club_metadata"))
    print("Saved club_metadata.")

    print("Stage 3 Complete. Query-optimized tables saved.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python transform/execute.py <input_file_path> <output_directory>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]

    spark = create_spark_session()

    cleaned_players_df = load_and_clean_players(spark, input_file, output_dir)
    create_master_table(cleaned_players_df, output_dir)
    create_query_tables(cleaned_players_df, output_dir)

    print("Transformation pipeline completed successfully.")
    spark.stop()