# Find the top 5 businesses with most reviews. Assume that each row has a unique business_id such that the total reviews for each business is listed on each row. 
# Output the business name along with the total number of reviews and order your results by the total reviews in descending order.

import pyspark

yelp_business = yelp_business.orderBy('review_count',ascending=False).select('name','review_count').limit(5)

yelp_business.toPandas()
