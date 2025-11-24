from schema.schema import ALLOWED_EVENT_TYPES

from pyspark.sql.functions import col, expr, to_date, hour, row_number, lit
from pyspark.sql.window import Window


def load_bronze(spark, bronze_path):
    return spark.read.parquet(bronze_path)


def parse_timestamp(df):
    df2 = df.withColumn(
        'event_ts_ts',
        expr('try_cast(event_ts as timestamp)')
    ).withColumn(
        'event_date',
        to_date(col('event_ts_ts'))
    ).withColumn(
        'event_hour',
        hour(col('event_ts_ts'))
    )
    return df2

def deduplicate_events(df):
    w = Window.partitionBy('event_id').orderBy(col('ingest_ts').desc_nulls_last())
    deduped_df = df.withColumn('rn', row_number().over(w)).filter(col('rn')==1).drop('rn')
    return deduped_df

def validate_and_quarantine(df):
    bad_ts = col("event_ts_ts").isNull()
    bad_user = col("user_id").isNull()

    # Build OR-chain for allowed events
    valid_event_condition = None
    for e in ALLOWED_EVENT_TYPES:
        condition = (col("event_type") == lit(e))
        valid_event_condition = (
            condition if valid_event_condition is None 
            else (valid_event_condition | condition)
        )

    invalid_event = ~valid_event_condition
    quarantine_filter = bad_ts | bad_user | invalid_event

    df_quarantine = df.filter(quarantine_filter)
    df_valid = df.filter(~quarantine_filter)

    return df_valid, df_quarantine
    

def write_quarantine(df_quarantine, quarantine_path):
    if df_quarantine.rdd.isEmpty():
        return 0
    df_quarantine.write.mode('append').parquet(quarantine_path)
    return df_quarantine.count()

def write_clean_checkpoint(df_clean, out_path):
    df_clean.write.mode('overwrite').partitionBy('event_date').parquet(out_path)


def run_phase1(spark, bronze_path, clean_out_path, quarantine_path):
    df_raw = load_bronze(spark, bronze_path)
    df_parsed = parse_timestamp(df_raw)
    df_dedup = deduplicate_events(df_parsed)
    df_valid, df_quarantine = validate_and_quarantine(df_dedup)

    if df_quarantine.count()>0:
        write_quarantine(df_quarantine, quarantine_path)

    write_clean_checkpoint(df_valid, clean_out_path)

    return df_valid, df_quarantine


