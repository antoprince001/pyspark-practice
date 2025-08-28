# Calculate the percentage of users who are both from the US and have an 'open' status, as indicated in the fb_active_users table.

import pyspark.sql.functions as F

counts = fb_active_users.agg(
    F.count("*").alias("total"),
    F.sum(F.when((F.col("country") == "USA") & (F.col("status") == "open"), 1).otherwise(0)).alias("usa_open")
).collect()[0]

fb_active_users_percentage = (counts["usa_open"] * 100.0) / counts["total"]
fb_active_users_percentage
