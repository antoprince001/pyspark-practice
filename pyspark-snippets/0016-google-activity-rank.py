# Find the email activity rank for each user. Email activity rank is defined by the total number of emails sent. 
# The user with the highest number of emails sent will have a rank of 1, and so on. 
# Output the user, total emails, and their activity rank. Order records by the total emails in descending order.
# Sort users with the same number of emails in alphabetical order.
# In your rankings, return a unique value (i.e., a unique rank) even if multiple users have the same number of emails. 
# For tie breaker use alphabetical order of the user usernames.

import pyspark
from pyspark.sql.functions import count, row_number
from pyspark.sql import Window

google_gmail_emails = google_gmail_emails.groupby('from_user')\
    .agg(count('id').alias('total_emails'))
    
google_gmail_emails = google_gmail_emails.withColumn("row_number", row_number().over(Window.orderBy(google_gmail_emails.total_emails.desc(),google_gmail_emails.from_user.asc())))
 

google_gmail_emails.toPandas()
