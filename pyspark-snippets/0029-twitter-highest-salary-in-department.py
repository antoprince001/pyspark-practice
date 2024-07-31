# Find the employee with the highest salary per department.
# Output the department name, employee's first name along with the corresponding salary.

# Import your libraries
import pyspark
from pyspark.sql.functions import col, max
# Start writing code
dept_salary = employee.groupBy(employee.department).agg(max(col("salary")).alias("max_salary"))

employee = employee.alias("e")
dept_salary = dept_salary.alias("ds")

employee = employee.join(dept_salary, 
                       (col("e.department") == col("ds.department")) & 
                       (col("e.salary") == col("ds.max_salary")), 
                       "inner") \
                 .select(col("e.department"), col("e.first_name"), col("ds.max_salary"))

# To validate your solution, convert your final pySpark df to a pandas df
employee.toPandas()
