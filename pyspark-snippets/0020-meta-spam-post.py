# Calculate the percentage of spam posts in all viewed posts by day. A post is considered a spam if a string "spam" is inside keywords of the post. 
# Note that the facebook_posts table stores all posts posted by users. The facebook_post_views table is an action table denoting if a user has viewed a post.

import pyspark
from pyspark.sql.functions import count, when

facebook_posts = facebook_posts.join(facebook_post_views, 'post_id','inner').groupby('post_date').agg(100*count(when(facebook_posts.post_keywords.contains('spam'),facebook_post_views.viewer_id))/count(facebook_post_views.post_id))

facebook_posts.toPandas()
