# Write a query that returns the number of unique users per client per month

import pyspark
from pyspark.sql.functions import month, countDistinct

fact_events = fact_events.groupby("client_id", month("time_id")).agg(countDistinct(fact_events.user_id))

fact_events.toPandas()
