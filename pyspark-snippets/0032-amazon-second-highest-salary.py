# Import your libraries
import pyspark
from pyspark.sql import Window
from pyspark.sql.functions import dense_rank, col, desc

window = Window.orderBy(desc("salary"))

# Start writing code

employee = employee.withColumn("drank", dense_rank().over(window))
employee = employee.filter(col("drank") == 2).select("salary").distinct()
# To validate your solution, convert your final pySpark df to a pandas df
employee.toPandas()
