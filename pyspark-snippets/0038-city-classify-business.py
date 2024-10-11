# Classify each business as either a restaurant, cafe, school, or other.
# Output the business name and their classification.

import pyspark
from pyspark.sql.functions import when

df = sf_restaurant_health_violations.select('business_name').distinct()

sf_restaurant_health_violations = df.withColumn('classification',when(df.business_name.ilike('%restaurant%'),'restaurant').when((df.business_name.ilike('%cafe%') | df.business_name.ilike('%café%') | df.business_name.ilike('%coffee%')),'cafe').when(df.business_name.ilike('%school%'),'school').otherwise('other'))

sf_restaurant_health_violations.toPandas()
