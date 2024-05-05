# Find the average total compensation based on employee titles and gender. Total compensation is calculated by adding both the salary and bonus of each employee. 
# However, not every employee receives a bonus so disregard employees without bonuses in your calculation. Employee can receive more than one bonus.
import pyspark
from pyspark.sql.functions import col, expr, lit

sf_bonus = sf_bonus.groupby('worker_ref_id').sum('bonus').select('worker_ref_id', col('sum(bonus)').alias('total_bonus'))

sf_employee = sf_employee.join(sf_bonus, sf_employee.id == sf_bonus.worker_ref_id, 'inner').withColumn('total_compensation', col('salary') + col('total_bonus')).groupby('employee_title','sex').avg('total_compensation')

sf_employee.toPandas()
