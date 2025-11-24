# Schema and value pools for event generator

import uuid 
import datetime
#import pyspark

ALLOWED_EVENT_TYPES = ('click','search','play','pause','purchase','signup','logout')

SCHEMA_DICT = {
    'event_ts': {
        'type':'ISO8601 string',
        'meaning':'UTC timestamp when event occured'},
    'event_id':{
        'type':'string (UUID)',
        'meaning':'Unique identifier for the event; used for deduplication'
    },
    'user_id':{
        'type':'string (stable user identifier)',
        'meaning':'Persistent user identifier (not an IP). Use UUID or platform user id.'

    },
    'session_id':{
        'type':'string',
        'meaning':'Session identifier grouping events for a single session'
    },
    'event_type':{
        'type':'string (enum)',
        'meaning':'One of the allowed event types such as click, search, play, pause, purchase, signup, logout'
    },
    'page':{
        'type':'string',
        'meaning':'Page or screen where the event occurred (e.g., products_page, search_result, home, video_page)'
    },
    'device':{
        'type':'struct',
        'meaning':'Nested object containing device metadata',
        'fields':{
            'device_type':{
                'type':'string',
                'meaning':'mobile,desktop,tablet,tv'
            },
            'os':{
                'type':'string',
                'meaning':'iOS,Windows,Android,macOS'
            },
            'browser':{
                'type':'string',
                'meaning':'Chrome,Safari,Firefox,Edge'
            }

        }
    },
    'geo':{
        'type':'struct',
        'meaning':'Nested object containing geolocation info',
        'fields':{
            'country':{
                'type':'string',
                'meaning':'IN,US'
            },
            'region':{
                'type':'string',
                'meaning':'HR,MH,DL,UK',
            },
            'city':{
                'type':'string',
                'meaning':'Mumbai,Delhi,Pune,Haridwar'
            },
            'ip':{
                'type':'string',
                'meaning':'IP address'
            }
        }
    },
    'props':{
        'type':'map<string, string/number>',
        'meaning':'Event-specific key/value attributes (e.g., video_id, search_query, duration, clicked_element, content_category, product_id, amount)'
    },
    'user_props':{
        'type':'struct',
        'meaning':'Snapshot of user attributes useful for segmentation (plan, signup_date, referral_source)'
    }
}

DEVICE_POOLS = {
    'device_types': ['mobile','desktop','tablet','tv'],
    'oses': ['iOS','Windows','Android','macOS'],
    'browsers': ['Chrome','Safari','Firefox','Edge','App']
}

GEO_POOLS = {
    'countries': ['IN','US'],
    'regions': ['HR','MH','DL','UK'],
    'cities': ['Mumbai','Delhi','Pune','Haridwar']
}

SAMPLE_USERS = [f'user_{x}' for x in range(200)]

DEFAULT_PROPS_BY_EVENT = {
    'search': ['search_query'],
    'play': ['video_id','duration'],
    'click': ['clicked_element','content_category'],
    'purchase': ['product_id','amount'],
    'signup': [],
    'pause':[],
    'logout':[]
}



SCHEMA_NOTE = ("Schema describes the project clickstream event shape. "
               "user_id is a persistent, stable identifier for the user (do not use IP for user identity). "
               "The IP address is stored as geo.ip for geolocation. "
               "props is a flexible key/value bag for event-specific attributes (e.g., video_id, duration, search_query). "
               "user_props is an optional snapshot of user metadata used for segmentation and feature engineering.")

# SPARK STRUCTTYPE OUTLINE (for later ETL)
# event_ts     -> TimestampType()         # parsed from ISO8601 string into Spark timestamp
# event_id     -> StringType()
# user_id      -> StringType()
# session_id   -> StringType()
# event_type   -> StringType()
# page         -> StringType()
# device       -> StructType([
#                    StructField("device_type", StringType()),
#                    StructField("os", StringType()),
#                    StructField("browser", StringType())
#                ])
# geo          -> StructType([
#                    StructField("country", StringType()),
#                    StructField("region", StringType()),
#                    StructField("city", StringType()),
#                    StructField("ip", StringType())
#                ])
# props        -> MapType(StringType(), StringType())  # or MapType(StringType(), StringType(), True) for nullable values
# user_props   -> StructType([
#                    StructField("plan", StringType()),
#                    StructField("signup_date", DateType()),  # or StringType if you prefer
#                    StructField("referral_source", StringType())
#                ])
# Note: For initial parsing you can read event_ts as StringType and cast to TimestampType in ETL if preferred.
