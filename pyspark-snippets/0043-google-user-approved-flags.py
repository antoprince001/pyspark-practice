# Which user flagged the most distinct videos that ended up approved by YouTube? Output, in one column, their full name or names in case of a tie. In the user's full name, include a space between the first and the last name.

# Import your libraries
import pyspark.sql.functions as F

flag_review = flag_review.filter((flag_review.reviewed_by_yt == 1) & (flag_review.reviewed_outcome == 'APPROVED'))
user_flags = user_flags.join(flag_review, user_flags.flag_id == flag_review.flag_id).drop(user_flags.flag_id)

user_flags_with_name = user_flags.withColumn('full_name',F.concat_ws(" ",user_flags.user_firstname, user_flags.user_lastname))

user_flags_with_name = user_flags_with_name.groupBy(F.col('full_name')).agg(
    F.countDistinct(F.col('video_id')).alias('count_of_videos'))
    
user_flags_with_name = user_flags_with_name.orderBy('count_of_videos',ascending=False)

max_count = user_flags_with_name.select(F.max('count_of_videos')).collect()[0][0]

user_flags_with_name = user_flags_with_name.filter(user_flags_with_name.count_of_videos == max_count ).select(user_flags_with_name.full_name.alias('user_name'))
user_flags_with_name.toPandas()
