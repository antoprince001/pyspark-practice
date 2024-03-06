# Compare each employee's salary with the average salary of the corresponding department.

import pyspark
from pyspark.sql.window import Window
from pyspark.sql.functions import avg

window =  Window.partitionBy("department")
employee = employee.withColumn('avg', avg("salary").over(window)).select('department','first_name','salary','avg')

employee.toPandas()

