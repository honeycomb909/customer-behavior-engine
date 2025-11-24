from pyspark.sql.functions import *

def transform_bronze(df_raw):
    df_bronze = df_raw.withColumn(
        'event_ts_ts',to_timestamp(col('event_ts'))
    ).withColumn(
        'ingest_ts',current_timestamp()
    ).withColumn(
        'source_file',input_file_name()
    ).withColumn(
        'event_date',to_date(col('event_ts_ts'))
    ).withColumn(
        'event_hour',hour(col('event_ts_ts'))
    )
    return df_bronze


