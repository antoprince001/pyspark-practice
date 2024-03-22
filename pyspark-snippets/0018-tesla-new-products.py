# You are given a table of product launches by company by year. Write a query to count the net difference between the number of products companies launched in 2020 with the number of products companies launched in the previous year. 
# Output the name of the companies and a net difference of net products released for 2020 compared to the previous year.

import pyspark
from pyspark.sql.functions import countDistinct,col, expr

car_launches_2020 = car_launches.filter(car_launches.year == 2020).groupby('company_name').agg(countDistinct('product_name').alias('count_products'))

car_launches_2019 = car_launches.filter(car_launches.year == 2019).groupby('company_name').agg(countDistinct('product_name').alias('count_products'))

car_launches = car_launches_2020.join(car_launches_2019,'company_name','inner').withColumn('net_products',car_launches_2020.count_products-car_launches_2019.count_products).select('company_name','net_products')
car_launches.toPandas()
