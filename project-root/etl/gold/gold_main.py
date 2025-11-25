import sys, os

CURRENT = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(CURRENT))
sys.path.append(ROOT)
 
from pyspark.sql import SparkSession
from etl.gold.gold_dim import (
    create_dim_user, 
    create_dim_device, 
    create_dim_geo,
    create_dim_time
)

from etl.gold.gold_transform import create_fact_events


spark = SparkSession.builder.appName('goldETL').getOrCreate()

silver_clean_path = 'data/silver/staging/bronze_clean/'
gold_output_path = 'data/gold/'

silver_norm_path = 'data/silver/normalized/'

df_norm = spark.read.parquet(silver_norm_path)

df_user = create_dim_user(df_norm)
df_device = create_dim_device(df_norm)
df_geo = create_dim_geo(df_norm)
df_time = create_dim_time(df_norm)


df_user.write.mode('overwrite').parquet(gold_output_path+"dim_user/")
df_device.write.mode('overwrite').parquet(gold_output_path+"dim_device/")
df_geo.write.mode('overwrite').parquet(gold_output_path+"dim_geo/")
df_time.write.mode('overwrite').parquet(gold_output_path+"dim_time/")


fact = create_fact_events(df_norm, df_user, df_device, df_geo, df_time)

fact.write.mode('overwrite').parquet(gold_output_path+'gold_fact')


spark.stop()




