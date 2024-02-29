# Meta/Facebook has developed a new programing language called Hack.To measure the popularity of Hack they ran a survey with their employees. The survey included data on previous programing familiarity as well as the number of years of experience, age, gender and most importantly satisfaction with Hack. Due to an error location data was not collected, but your supervisor demands a report showing average popularity of Hack by office location. Luckily the user IDs of employees completing the surveys were stored.
# Based on the above, find the average popularity of the Hack per office location.
# Output the location along with the average popularity.


import pyspark
from pyspark.sql.functions import  avg

facebook_employees = facebook_employees.join(facebook_hack_survey,facebook_employees.id == facebook_hack_survey.employee_id ,'inner').groupBy('location').avg('popularity')

facebook_employees.toPandas()


facebook_employees = facebook_employees \
    .join(facebook_hack_survey, facebook_employees.id == facebook_hack_survey.employee_id, 'inner') \
    .groupBy('location') \
    .avg('popularity')
