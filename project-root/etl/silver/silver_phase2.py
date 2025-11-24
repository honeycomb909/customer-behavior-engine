import os, sys
from pyspark.sql import SparkSession

CURRENT = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(CURRENT))
sys.path.append(ROOT)

from etl.silver.silver_transform import normalize_bronze, extract_props


def run_phase2(spark, clean_silver_path):
    df_clean = spark.read.parquet(clean_silver_path)

    df_norm = normalize_bronze(df_clean)
    df_props = extract_props(df_clean)

    return df_norm, df_props