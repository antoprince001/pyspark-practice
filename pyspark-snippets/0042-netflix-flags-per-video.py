# For each video, find how many unique users flagged it. A unique user can be identified using the combination of their first name and last name. Do not consider rows in which there is no flag ID.

# Import your libraries
import pyspark.sql.functions as F

# Start writing code
user_flags = user_flags.filter(user_flags.flag_id.isNotNull()).withColumn('full_name', F.concat_ws(" ", F.col("user_firstname"), F.col("user_lastname"))).groupBy('video_id').agg(F.countDistinct('full_name'))

# To validate your solution, convert your final pySpark df to a pandas df
user_flags.toPandas()


# concat_ws(" ", ...) automatically handles null values more gracefully (skips them). F.concat(...) returns null if any part is null.
# better to separate operations
