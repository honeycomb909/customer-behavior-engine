import sys, os

# add project-root to PYTHONPATH
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
sys.path.append(PROJECT_ROOT)

from pyspark.sql import SparkSession
from etl.bronze.bronze_write import write_bronze
from etl.bronze.bronze_transform import transform_bronze

spark = SparkSession.builder.appName("BronzeETL").getOrCreate()

raw_path = "data/raw/date=*/hour=*/*.ndjson"
output_path = "data/bronze/"

df_raw = spark.read.json(raw_path)
df_bronze = transform_bronze(df_raw)

write_bronze(df_bronze, output_path)
df_bronze.printSchema()