# Find the popularity percentage for each user on Meta/Facebook. The popularity percentage is defined as the total number of friends the user has divided by the total number of users on the platform, then converted into a percentage by multiplying by 100.
# Output each user along with their popularity percentage. Order records in ascending order by user id.
# The 'user1' and 'user2' column are pairs of friends.

import pyspark
from pyspark.sql.functions import count

facebook_friends_1 = facebook_friends.select('user2','user1')
facebook_friends = facebook_friends.unionAll(facebook_friends_1)

total_users = facebook_friends[['user1']].distinct().count()
facebook_friends = facebook_friends.groupby('user1').agg((count('user2')*100/total_users))
facebook_friends.toPandas()
