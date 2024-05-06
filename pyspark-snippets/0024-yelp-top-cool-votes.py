# Find the review_text that received the highest number of  'cool' votes.
# Output the business name along with the review text with the highest numbef of 'cool' votes.

import pyspark
import pyspark.sql.functions as F

highest_cool_value = yelp_reviews.select(F.max('cool')).collect()[0][0]
yelp_reviews = yelp_reviews.filter(yelp_reviews.cool == F.lit(highest_cool_value)).select('business_name', 'review_text')

yelp_reviews.toPandas()
