# run_pipeline.py

import time
from extract import extract_all_cities
from transform import transform_all
from load import create_table_if_not_exists, load_to_supabase
from etl_analysis import run_analysis
from pathlib import Path


def run_full_pipeline():

    print("\n🚀 STARTING FULL ETL PIPELINE\n")

    print("\n📡 STEP 1: Extracting Air Quality Data...")
    extract_all_cities()   # creates multiple raw JSON files
    time.sleep(1)
    print("\n🔧 STEP 2: Transforming Data...")
    staged_csv = transform_all()     # returns path to air_quality_transformed.csv
    print("\n📦 STEP 3: Loading into Supabase...")
    create_table_if_not_exists()
    load_to_supabase(staged_csv, batch_size=200)
    print("\n📊 STEP 4: Running Analysis...")
    run_analysis()

    print("\n🎉 FULL ETL PIPELINE FINISHED SUCCESSFULLY!\n")


if __name__ == "__main__":
    run_full_pipeline()
