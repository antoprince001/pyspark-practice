# Count the number of unique street names for each postal code in the business dataset. 
#Use only the first word of the street name, case insensitive (e.g., "FOLSOM" and "Folsom" are the same). 
#If the structure is reversed (e.g., "Pier 39" and "39 Pier"), count them as the same street. Output the results with postal codes, ordered by the number of streets (descending) and postal code (ascending).


import pyspark
from pyspark.sql.functions import split, col, lower, when, count_distinct, desc, lit,trim
sf_restaurant_health_violations = (
    sf_restaurant_health_violations
    .withColumn(
        'processed_street_address',
        split(lower(col('business_address')), ' ')
    )
    .withColumn(
        'processed_street_address',
        when(
            col('processed_street_address').getItem(0).rlike(r"^-?\d+$"),
            col('processed_street_address').getItem(1)
        ).otherwise(
            col('processed_street_address').getItem(0)
        )
    )
).where(col("business_postal_code").isNotNull()).groupBy('business_postal_code').agg(count_distinct(col('processed_street_address')).alias('n_streets')).orderBy(desc('n_streets'),'business_postal_code' )

sf_restaurant_health_violations.toPandas()
