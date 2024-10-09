# You're given a dataset of health inspections. Count the number of violation in an inspection in 'Roxanne Cafe' for each year. 
# If an inspection resulted in a violation, there will be a value in the 'violation_id' column. Output the number of violations by year in ascending order.

# Import your libraries
import pyspark

from pyspark.sql.functions import year,count, col
# Start writing code
sf_restaurant_health_violations = sf_restaurant_health_violations.withColumn('year',year('inspection_date'))

sf_restaurant_health_violations = sf_restaurant_health_violations.filter(col('business_name') == 'Roxanne Cafe').groupBy('year').agg(count('violation_id'))


# To validate your solution, convert your final pySpark df to a pandas df
sf_restaurant_health_violations.toPandas()
