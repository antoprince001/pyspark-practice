# Find the number of Apple product users and the number of total users with a device and group the counts by language. 
# Assume Apple products are only MacBook-Pro, iPhone 5s, and iPad-air. Output the language along with the total number of Apple users and users with any device. 
# Order your results based on the number of total users in descending order.

import pyspark
from pyspark.sql.functions import countDistinct,when

playbook_events = playbook_users.join(playbook_events,'user_id','inner') \
    .groupby('language').agg( \
    countDistinct(when((playbook_events.device.isin('macbook pro', 'iphone 5s', 'ipad air')), playbook_events.user_id)).alias('n_apple_users'),
        countDistinct(playbook_users.user_id).alias('n_total_user')
    )
playbook_events = playbook_events.orderBy('n_total_users', ascending=False)

playbook_events.toPandas()
