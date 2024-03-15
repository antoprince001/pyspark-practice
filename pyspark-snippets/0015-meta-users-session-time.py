# Calculate each user's average session time. A session is defined as the time difference between a page_load and page_exit. 
# For simplicity, assume a user has only 1 session per day and if there are multiple of the same events on that day, 
# consider only the latest page_load and earliest page_exit, with an obvious restriction that load time event should happen before exit time event . 
# Output the user_id and their average session time.



import pyspark
import pyspark.sql.functions as F

facebook_web_log = facebook_web_log.filter(F.col('action') == 'page_load').select('user_id', 'timestamp').alias('load') \
    .join(facebook_web_log.filter(F.col('action') == 'page_exit').select('user_id', 'timestamp').alias('exit'),
'user_id', 'left') \
    .filter(F.col('load.timestamp') < F.col('exit.timestamp')) \
    .withColumn('date_load', F.to_date(F.col('load.timestamp'))) \
    .groupBy('user_id', 'date_load') \
    .agg(F.max('load.timestamp').alias('timestamp_load'), F.min('exit.timestamp').alias('timestamp_exit')) \
    .withColumn('duration', F.expr('timestamp_exit - timestamp_load')) \
    .filter(F.col('duration') > F.expr('INTERVAL 0 days')) \
    .groupBy('user_id') \
    .agg(F.mean('duration').alias('duration')) \

facebook_web_log.toPandas()
