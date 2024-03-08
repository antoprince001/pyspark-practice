# Find the number of employees working in the Admin department that joined in April or later.

import pyspark
from pyspark.sql.functions import count, col, month

worker = worker.filter((worker.joining_date >= '2014-04-01') & (worker.department == 'Admin')).count()

worker = worker.filter((month(worker.joining_date) >= 4) & (worker.department == 'Admin')).count()
