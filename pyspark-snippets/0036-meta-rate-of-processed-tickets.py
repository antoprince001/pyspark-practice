-- Find the rate of processed tickets for each type.

import pyspark
from pyspark.sql.functions import when,sum,count

facebook_complaints = facebook_complaints.groupby('type').agg(sum(when(facebook_complaints.processed == True,1.0).otherwise(0))/count(facebook_complaints.processed))

facebook_complaints.toPandas()
