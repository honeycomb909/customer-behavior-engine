from pyspark.sql.functions import *
from pyspark.sql.window import Window


def create_dim_user(df):
    df_user = df.select(
        col('user_id')
    ).dropDuplicates()
    return df_user

def create_dim_geo(df):
    return df.select(
        col('geo_country'),col('geo_region'),col('geo_city')
    ).dropDuplicates()
   
    



def create_dim_device(df):
    return df.select(
        col('device_type'),col('device_os'),col('device_browser')
    ).dropDuplicates()



def create_dim_time(df):
    return df.select(
        col('event_date'),
        col('event_hour')
    ).withColumn('year',year(col('event_date'))) \
    .withColumn('month',month(col('event_date'))) \
    .withColumn('day_of_month',dayofmonth(col('event_date'))) \
    .withColumn('day_of_week',dayofweek(col('event_date'))) \
    .dropDuplicates()
                 


