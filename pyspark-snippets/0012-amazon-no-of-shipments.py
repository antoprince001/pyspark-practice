# Write a query that will calculate the number of shipments per month. The unique key for one shipment is a combination of shipment_id and sub_id. 
# Output the year_month in format YYYY-MM and the number of shipments in that month.

import pyspark.sql.functions as F

amazon_shipment = amazon_shipment.withColumn('year_month', F.date_format(F.col('shipment_date'), 'yyyy-MM'))
amazon_shipment = amazon_shipment.withColumn('unique_key', F.concat(F.col('shipment_id').cast('string'), F.lit('_'), F.col('sub_id').cast('string')))
result = amazon_shipment.groupby('year_month').agg(F.countDistinct('unique_key').alias('count')).orderBy('year_month').toPandas()

result
