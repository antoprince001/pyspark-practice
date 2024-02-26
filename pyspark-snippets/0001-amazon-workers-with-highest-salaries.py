# You have been asked to find the job titles of the highest-paid employees.

import pyspark
from pyspark.sql.functions import col, max, when,lit

# # Start writing code
max_salary = worker.select(max('salary').alias('max_salary')).collect()[0].max_salary
worker = worker.filter( col("salary") == lit(max_salary))

worker = worker.join(title, worker.worker_id == title.worker_ref_id, "inner").select("worker_title")

worker.toPandas()


#Provided solution

import pyspark.sql.functions as F

title_worker_id = title.withColumnRenamed("worker_ref_id", "worker_id")
merged_df = worker.join(title_worker_id, on="worker_id")
max_salary = merged_df.filter(F.col("salary") == merged_df.select(F.max("salary")).first()[0]).select("worker_title").withColumnRenamed("worker_title", "best_paid_title")
result = max_salary.toPandas()
