from pyspark.sql.functions import countDistinct


def hourly_active_users(gold_fact):
    hau = gold_fact.groupBy('event_date','event_hour').agg(countDistinct('user_id').alias('hau'))
    hau.write.mode('append').parquet('data/analytics/hau/')
    return hau