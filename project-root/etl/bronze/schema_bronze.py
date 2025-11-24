from pyspark.sql.types import (
    StructType, 
    StructField,
    StringType,
    MapType
)


device_schema = StructType([
    StructField('device_type',StringType()),
    StructField('os',StringType()),
    StructField('browser',StringType())
])

geo_schema = StructType([
    StructField('country',StringType()),
    StructField('region',StringType()),
    StructField('city',StringType()),
    StructField('ip',StringType())
    
])

user_props_schema = StructType([
    StructField('plan',StringType()),
    StructField('signup_date',StringType()),
    StructField('referral_source',StringType())
])

props_schema = MapType(StringType(),StringType())

event_schema = StructType([
    StructField("event_ts", StringType()),
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_type", StringType()),
    StructField("page", StringType()),

    StructField("device", device_schema),
    StructField("geo", geo_schema),
    StructField("props", props_schema),
    StructField("user_props", user_props_schema),
])


