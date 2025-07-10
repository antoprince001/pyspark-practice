# Identify customers who did not place an order between 2019-02-01 and 2019-03-01.
# •    Customers who placed orders only outside this date range.
# •    Customers who never placed any orders.

# Import your libraries
import pyspark.sql.functions as F

# Start writing code

orders = orders.filter((orders.order_date >= '2019-02-01') & (orders.order_date <= '2019-03-01' )).select(F.col('cust_id')).distinct()
customer_ids = customers.selectExpr("id as cust_id").exceptAll(orders).select('cust_id')

customers = customers.join(customer_ids,customers.id == customer_ids.cust_id, 'inner' ).select('first_name')

# To validate your solution, convert your final pySpark df to a pandas df
customers.toPandas()


from pyspark.sql.functions import col, to_date, lit

# Filter orders within the date range
orders_in_range = orders.filter(
    (col("order_date") >= to_date(lit("2019-02-01"), "yyyy-MM-dd")) &
    (col("order_date") <= to_date(lit("2019-03-01"), "yyyy-MM-dd"))
).select("cust_id").distinct()

# Perform a left anti join to find customers not in the date range
customers_not_in_range = customers.join(
    orders_in_range,
    customers["id"] == orders_in_range["cust_id"],
    "left_anti"
).select("first_name")

# Convert to Pandas DataFrame
result = customers_not_in_range.toPandas()

# Joins: left, left_anti, inner — core operations in distributed processing

# Broadcast Join: If customers is small, Spark can broadcast it to all workers (optimize performance)

# Predicate Pushdown: The filter() applied on orders can be pushed down to the source (if supported) to avoid unnecessary I/O

# Shuffling: Avoided in left_anti if broadcast is enabled; introduced in exceptAll and some left join cases


