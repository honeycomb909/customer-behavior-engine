from pyspark.sql.functions import *


def funnel_metrics(df):
    df_search = df.filter(col('event_type')=='search').groupBy('event_date').agg(count('*').alias('search_count'))
    df_click = df.filter(col('event_type')=='click').groupBy('event_date').agg(count('*').alias('click_count'))
    df_play = df.filter(col('event_type')=='play').groupBy('event_date').agg(count('*').alias('play_count'))
    df_purchase = df.filter(col('event_type')=='purchase').groupBy('event_date').agg(count('*').alias('purchase_count'))
    funnel = df_search.join(df_click,'event_date','left') \
        .join(df_play,'event_date','left')\
        .join(df_purchase,'event_date','left')
    funnel.write.mode('append').parquet('data/analytics/funnel/')
    return funnel

    














