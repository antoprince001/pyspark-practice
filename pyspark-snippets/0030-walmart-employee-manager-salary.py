# Find employees who are earning more than their managers. Output the employee's first name along with the corresponding salary.

import pyspark
from pyspark.sql.functions import col

employee_a = employee.alias('a')
employee_b = employee.alias('b')

joined_df = employee_a.join(employee_b, col("a.manager_id") == col("b.id"), 'inner').filter(col("a.salary") > col("b.salary")).select(col("a.first_name"),col("a.salary")).distinct()

joined_df.toPandas()
