# Write a query that calculates the difference between the highest salaries found in the marketing and engineering departments. Output just the absolute difference in salaries.


import pyspark
from pyspark.sql.functions import * 

df = db_employee.join(db_dept, db_employee.department_id == db_dept.id,'inner').filter(db_dept.department.isin('engineering','marketing')).groupby(db_dept.department).agg(max(col("salary")).alias('salary'))


diff = df.select('salary').filter("department = 'marketing'").first()[0] - df.select('salary').filter("department = 'engineering'").first()[0]

# diff.toPandas()

