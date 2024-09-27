# Make a report showing the number of survivors and non-survivors by passenger class.
# Classes are categorized based on the pclass value as:
# pclass = 1: first_class
# pclass = 2: second_classs
# pclass = 3: third_class
# Output the number of survivors and non-survivors by each class.

import pyspark
from pyspark.sql.functions import sum,when,col,lit

titanic = titanic.groupBy('survived').agg(\
    sum(when(col('pclass') == lit(1),lit(1)).otherwise(lit(0))).alias('first_class'),
    sum(when(col('pclass') == lit(2),lit(1)).otherwise(lit(0))).alias('second_class'),
    sum(when(col('pclass') == lit(3),lit(1)).otherwise(lit(0))).alias('third_class'),
)

titanic.toPandas()



import pyspark.sql.functions as F

grouped = titanic.groupby(['pclass','survived']).agg(F.count('*').alias('count'))
pivot = grouped.groupBy('survived').pivot('pclass').agg(F.sum('count'))
pivot = pivot.withColumnRenamed('1', 'first_class').withColumnRenamed('2', 'second_class').withColumnRenamed('3', 'third_class')
result = pivot.toPandas()
result

# If your use case is simple and specific to counting occurrences of passengers by class and survival, the first approach might be easier to understand and maintain.
# If you plan to pivot or have more complex groupings (e.g., more classes or dimensions), or if your dataset is large and performance is a concern, the second approach using pivot will be more efficient and flexible.
# In general, the pivot-based approach is often better for handling complex data structures or when working with dynamic datasets.
