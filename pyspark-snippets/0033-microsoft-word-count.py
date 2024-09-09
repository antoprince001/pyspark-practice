# Find the number of times each word appears in drafts.

import pyspark
from pyspark.sql.functions import col,lower, split, explode,regexp_replace,count 

google_file_store = google_file_store.filter(col('filename').ilike('%draft%'))

google_file_store = google_file_store.select(
    explode(split(
        lower(regexp_replace(regexp_replace(col('contents'), ',', ''), '\\.', '')), 
        ' '
    )).alias('word')
).groupBy('word').agg(count('word').alias('nentry'))
google_file_store.toPandas()


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Filter rows where the filename contains 'draft'
draft = google_file_store.filter(google_file_store.filename.contains('draft'))

# Split the contents into words, explode the array into multiple rows and count occurrences
result = (draft
          .withColumn("words", explode(split(col("contents"), "\W+")))
          .groupBy("words")
          .count()
          .filter(col("words") != "")  # Filter out empty strings
          .withColumnRenamed("words", "word")
          .withColumnRenamed("count", "occurrences")
          .toPandas()  # Convert to Pandas DataFrame if necessary
         )

result
