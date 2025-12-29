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


✔️ Joins → ✔️ Select required columns → ✔️ Derived columns
