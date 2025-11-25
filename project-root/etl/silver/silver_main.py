import sys, os
from pyspark.sql import SparkSession

# Add project-root to Python path
CURRENT = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(CURRENT))
sys.path.append(ROOT)

from etl.silver.silver_phase1 import run_phase1
from etl.silver.silver_phase2 import run_phase2


def main():

    spark = (
        SparkSession.builder
        .appName("SilverETL")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Input & output paths
    bronze_path = "data/bronze/"                      # from Bronze ETL
    clean_silver_path = "data/silver/clean/"          # after Phase-1
    quarantine_path = "data/quarantine/bronze/"       # from Phase-1
    norm_path = "data/silver/normalized/"             # normalized output
    props_path = "data/silver/props/"                 # extracted props

    # -------- Phase-1: Clean + Deduplicate + Validate --------
    df_clean, df_quarantine = run_phase1(
        spark,
        bronze_path,
        clean_silver_path,
        quarantine_path
    )

    print("Phase 1 complete:")
    df_clean.printSchema()
    print("Quarantine count:", df_quarantine.count())

    df_norm, df_props = run_phase2(spark, clean_silver_path)

    # Join normalized + props into a unified silver table
    df_final_silver = df_norm.join(df_props, on=["event_id"], how="left")

    # Write outputs
    df_final_silver.write.mode("overwrite").parquet(norm_path)
    df_props.write.mode("overwrite").parquet(props_path)

    print("Phase 2 complete:")
    df_norm.printSchema()
    df_props.printSchema()

    print("Silver ETL finished successfully.")


if __name__ == "__main__":
    main()