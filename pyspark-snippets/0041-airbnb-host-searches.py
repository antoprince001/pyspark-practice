# You are given a table named airbnb_host_searches that contains data for rental property searches made by users. 
# Determine the minimum, average, and maximum rental prices for each popularity-rating bucket. 
# A popularity-rating bucket should be assigned to every record based on its number_of_reviews (see rules below).



import pyspark.sql.functions as F


airbnb_host_searches = airbnb_host_searches.withColumn(
    "host_popularity",
    F.when(F.col('number_of_reviews') > 40, 'Hot')
    .when(F.col('number_of_reviews') >= 16, 'Popular')
    .when(F.col('number_of_reviews') >= 6, 'Trending Up')
    .when(F.col('number_of_reviews') >= 1, 'Rising')
    .otherwise('New')
)

airbnb_host_searches = airbnb_host_searches.groupBy('host_popularity').agg(
    F.min('price').alias('min_price'),
    F.avg('price').alias('avg_price'),
    F.max('price').alias('max_price')
)
# Add a columnm - poularity bucket , then group

airbnb_host_searches.toPandas()
