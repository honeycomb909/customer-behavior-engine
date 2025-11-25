from pyspark.sql.functions import *




def create_fact_events(df_norm, dim_user, dim_device, dim_geo, dim_time):
    fact = df_norm.join(dim_user, on='user_id',how='left')
    fact = fact.join(dim_device, on=['device_type','device_os','device_browser'],
                     how='left')
    fact = fact.join(dim_geo, on=['geo_country','geo_region','geo_city'],
                     how='left')
    fact = fact.join(dim_time, on=['event_date','event_hour'], how='left')

    return fact
