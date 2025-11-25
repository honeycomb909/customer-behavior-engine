from pyspark.sql.functions import col,count,avg,sum


def revenue_metrics(gold_fact):
    purchase_events = gold_fact.filter(col('event_type')=='purchase')
    revenue = purchase_events.groupBy('event_date').agg(count('*').alias('total_purchase'),sum(col('prop_amount')).alias('total_revenue'),avg(col('prop_amount')).alias('avg_revenue_per_purchase'))
    revenue.write.mode('append').parquet('data/analytics/revenue/')
    return revenue
