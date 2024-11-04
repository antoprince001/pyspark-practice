# What were the top 10 ranked songs in 2010?
# Output the rank, group name, and song name but do not show the same song twice.
# Sort the result based on the year_rank in ascending order.

import pyspark
from pyspark.sql.functions import col, lit

billboard_top_100_year_end = billboard_top_100_year_end.where((col('year') == lit(2010)) & (col('year_rank') < 11)).select('year_rank','group_name','song_name').distinct().orderBy('year_rank')

billboard_top_100_year_end.toPandas()
