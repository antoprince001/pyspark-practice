# Find order details made by Jill and Eva.
# Consider the Jill and Eva as first names of customers.
# Output the order date, details and cost along with the first name.
# Order records based on the customer id in ascending order.



# Import your libraries
import pyspark
from pyspark.sql import functions as F

# Start writing code
customers = customers \
    .filter(F.col('first_name').isin(['Jill', 'Eva'])) \
    .join(orders, customers['id'] == orders['cust_id']) \
    .select('first_name', 'order_date', 'order_details', 'total_order_cost') \
    .orderBy('cust_id')

# To validate your solution, convert your final pySpark df to a pandas df
customers.toPandas()
