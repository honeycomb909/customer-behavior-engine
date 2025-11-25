import sys, os

CURRENT = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(CURRENT))
sys.path.append(ROOT)


from etl.analytics.dau import daily_active_users
from etl.analytics.hau import hourly_active_users
from etl.analytics.event_type_metrics import event_type_distribution
from etl.analytics.revenue_metrics import revenue_metrics
from etl.analytics.funnel_metrics import funnel_metrics



from pyspark.sql.session import SparkSession


spark = SparkSession.builder.appName('analyticsETL').getOrCreate()

gold_input_path = 'data/gold/gold_fact/'

gold_fact = spark.read.parquet(gold_input_path)

daily_active_users(gold_fact)
hourly_active_users(gold_fact)
event_type_distribution(gold_fact)
revenue_metrics(gold_fact)
funnel_metrics(gold_fact)




spark.stop()

