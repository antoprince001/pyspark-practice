# Find libraries who haven't provided the email address in circulation year 2016 but their notice preference definition is set to email

import pyspark
from pyspark.sql.functions import col

library_usage = library_usage.filter((col('circulation_active_year') == 2016 ) & (col('notice_preference_definition')=='email') &(col('provided_email_address') == False)).select('home_library_code').distinct()

library_usage.toPandas()

# drop duplicates" allows you to specify a subset of columns to identify duplicates, while "distinct" operates on the entire row, removing completely identical rows.
# distinct when operating on entire row uses  a hash-based aggregation internally so optimized
