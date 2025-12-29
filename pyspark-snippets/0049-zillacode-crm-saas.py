# You are working on a Customer Relationship Management (CRM) software that manages information related to customers, orders, and products. 
# Write a function that combines these DataFrames and creates a column named 'customer_name' that should be the concatenation of the first name and last name of the customer, separated by a space. 



from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(customers, orders, products):
	# Write code here
    customers = customers.withColumn("customer_name", F.concat(F.col("first_name"),F.lit(' '),F.col("last_name")))
	  processed_df = orders.alias("ord").join(products.alias("prd"), F.col("ord.product_id") == F.col("prd.product_id"), "inner").select("category", "order_date","order_id","product_name","customer_id")
    processed_df = processed_df.alias("pdf").join(customers.alias("cust"),F.col("pdf.customer_id") == F.col("cust.customer_id"), "inner" ).select("category","customer_name","email", "order_date","order_id","product_name")
        
    processed_df.show()
    processed_df.explain()
    return processed_df


# ✔️ Joins → ✔️ Select required columns → ✔️ Derived columns



# 1. Partitioning: Properly partition the DataFrames to distribute the data evenly across the partitions. This can help minimize shuffling during the join operations, which can significantly improve performance. You can use the `repartition` or `partitionBy` functions to partition the DataFrames based on the join keys, such as 'customer_id' and 'product_id'.
    
# 2. Bucketing: If the join keys have high cardinality, you can use bucketing to group data in the DataFrames based on a hash function applied to the join key. This can help optimize the join operations, as Spark can avoid shuffling the data during joins. Use the `bucketBy` function when writing the DataFrames to create buckets.
    
# 3. Broadcasting: If one of the DataFrames is small enough to fit into the memory of each worker node, you can use the `broadcast` function to replicate the smaller DataFrame to all worker nodes. This can significantly speed up the join operations as Spark can perform map-side joins, eliminating the need for data shuffling. However, be cautious when using broadcasting, as it can cause memory issues if the DataFrame is too large to fit into the memory of the worker nodes.
    
# 4. Caching: Cache the intermediate results, especially if they are reused multiple times in the pipeline. Use the `cache` or `persist` functions to cache DataFrames in memory or on disk.
    
# 5. Column pruning: Select only the required columns as early as possible in the pipeline. This can help reduce the amount of data processed in subsequent operations.
    
# 6. Optimizing Spark configurations: Tune the Spark configurations based on the resources available in your cluster. Some important configurations include:
    
#     - `spark.executor.memory`: Increase the executor memory to allow for more data to be processed in-memory.
#     - `spark.executor.cores`: Set the number of cores per executor based on the available CPU resources.
#     - `spark.default.parallelism`: Set the default parallelism level based on the number of available cores and the size of the DataFrames.

# Remember to monitor and analyze the performance of the application using the Spark UI and various monitoring tools to identify bottlenecks and areas that require further optimization.
