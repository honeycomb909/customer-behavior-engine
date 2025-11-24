import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from etl.bronze.schema_bronze import event_schema

spark = (
    SparkSession.builder.appName("BronzeLayerIngestion")
         .config("spark.sql.shuffle.partitions", "200")
         .getOrCreate()
         )

spark.sparkContext.setLogLevel("WARN")

raw_path = "data/raw/date=*/hour=*/*.ndjson"


df_raw = spark.read.schema(event_schema).json(raw_path)


df_raw.printSchema()
df_raw.show(5,truncate=False)

df_raw.write.mode('append').partitionBy('event_date','event_hour').parquet()