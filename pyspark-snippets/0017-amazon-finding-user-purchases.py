# Write a query that'll identify returning active users. A returning active user is a user that has made a second purchase within 7 days of any other of their purchases. 
# Output a list of user_ids of these returning active users.


import pyspark

import pyspark.sql.functions as F


joined_df = amazon_transactions.alias("a").join(
    amazon_transactions.alias("b"),
    (F.col("a.user_id") == F.col("b.user_id")) & (F.abs(F.datediff(F.col("a.created_at"), F.col("b.created_at"))) <= 7) & (F.col("a.id") != F.col("b.id")),"inner")

result = joined_df.select("a.user_id").distinct().toPandas()
