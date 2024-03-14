# Find the 3 most profitable companies in the entire world.
# Output the result along with the corresponding company name.
# Sort the result based on profits in descending order.

import pyspark

forbes_global_2010_2014 = forbes_global_2010_2014.orderBy('profits',ascending=False).select('company','profits').limit(3)

forbes_global_2010_2014.toPandas()
