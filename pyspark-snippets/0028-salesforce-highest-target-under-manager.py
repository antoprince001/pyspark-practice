# Find the highest target achieved by the employee or employees who works under the manager id 13. Output the first name of the employee and target achieved. The solution should show the highest target achieved under manager_id=13 and which employee(s) achieved it.

import pyspark
from pyspark.sql.functions import max, col

max_value = salesforce_employees.filter(salesforce_employees['manager_id'] == 13).select(max(col('target')).alias('max_target')).collect()[0].max_target
salesforce_employees = salesforce_employees.filter((col('manager_id') == 13) & (col('target') == max_value)).select('first_name','target')

salesforce_employees.toPandas()
