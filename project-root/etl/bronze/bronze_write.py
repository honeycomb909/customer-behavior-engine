from pyspark.sql.functions import *


def write_bronze(df_bronze, output_path):
    df_bronze.write.mode('append')\
    .partitionBy('event_date','event_hour')\
    .parquet(output_path)  




