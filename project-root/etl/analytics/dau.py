from pyspark.sql.functions import countDistinct


def daily_active_users(gold_fact):
    dau = gold_fact.groupBy('event_date').agg(countDistinct('user_id').alias('dau'))
    dau.write.mode('append').parquet('data/analytics/dau/')
    return dau