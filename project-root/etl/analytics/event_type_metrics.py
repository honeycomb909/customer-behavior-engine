from pyspark.sql.functions import count


def event_type_distribution(gold_fact):
    event_count = gold_fact.groupBy('event_date','event_type').agg(count('*').alias('event_count'))
    event_count.write.mode('append').parquet('data/analytics/event_type/')
    return event_count





