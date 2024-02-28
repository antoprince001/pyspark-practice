#Find all posts which were reacted to with a heart. For such posts output all columns from facebook_posts table.

# Import your libraries
import pyspark
from pyspark.sql.functions import col

# Start writing code
facebook_reactions = facebook_reactions.filter(col('reaction') == 'heart')

facebook_reactions = facebook_reactions.join(facebook_posts, facebook_reactions.post_id == facebook_posts.post_id, 'inner').select(facebook_posts.post_id, facebook_posts.poster, "post_text", "post_keywords", facebook_posts.post_date).distinct()
# To validate your solution, convert your final pySpark df to a pandas df
facebook_reactions.toPandas()


# Import your libraries
import pyspark
from pyspark.sql.functions import col

# Start writing code
facebook_reactions = facebook_reactions.filter(col('reaction') == 'heart').select('post_id').distinct()

facebook_reactions = facebook_reactions.join(facebook_posts, facebook_reactions.post_id == facebook_posts.post_id, 'inner').select(facebook_posts.post_id, facebook_posts.poster, "post_text", "post_keywords", facebook_posts.post_date)
# To validate your solution, convert your final pySpark df to a pandas df
facebook_reactions.toPandas()
