# Your task is to write a function that takes in the input DataFrame and returns a DataFrame containing only the videos with more than 1,000,000 views and released in the last 5 years (assume current year is 2024). The output DataFrame should have the same schema as the input DataFrame.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
	# Write code here
    processed_df = input_df.where((F.col('release_year') > 2018) & (F.col('view_count') > 1000000))    
	processed_df.show()
    processed_df.explain()
    return processed_df


# filter() and where() are exactly the same.
# There is no performance or behavioral difference.
# Both compile to the same logical plan.
# where() exists mainly for SQL familiarity.
