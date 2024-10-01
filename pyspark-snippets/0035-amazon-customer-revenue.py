-- Calculate the total revenue from each customer in March 2019. Include only customers who were active in March 2019.
-- Output the revenue along with the customer id and sort the results based on the revenue in descending order.

import pyspark
from pyspark.sql.functions import sum

orders = orders.filter((orders.order_date >= '2019-03-01') & (orders.order_date <= '2019-03-31'))

orders = orders.groupby('cust_id').agg(sum('total_order_cost').alias('total_revenue')).orderBy(['total_revenue'], ascending = [False])

orders.toPandas()


import pyspark.sql.functions as F

orders = orders.withColumn('order_date', F.to_date(orders.order_date))
march = orders.filter(F.month(orders.order_date) == 3)
march_2019 = march.filter(F.year(march.order_date) == 2019)

result = march_2019.groupby('cust_id').agg(F.sum('total_order_cost').alias('revenue')).orderBy('revenue', ascending=False)
result.toPandas()
