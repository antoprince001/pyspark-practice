#Find the base pay for Police Captains

import pyspark

sf_public_salaries = sf_public_salaries.select('id','jobtitle','basepay','employeename').filter(sf_public_salaries.jobtitle.like('%CAPTAIN%POLICE%')).select('employeename','basepay')

sf_public_salaries.toPandas()


# The `contains()` function is often more optimized for simple substring matching than `like()` with `%` wildcards. 
# This could potentially lead to better performance, especially if your dataset is large.
