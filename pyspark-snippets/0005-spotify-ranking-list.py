# Find how many times each artist appeared on the Spotify ranking list
# Output the artist name along with the corresponding number of occurrences.
# Order records by the number of occurrences in descending order.

import pyspark
from pyspark.sql.functions import desc

spotify_worldwide_daily_song_ranking = spotify_worldwide_daily_song_ranking.groupBy('artist').count().alias('n_occurences').orderBy(desc("count"))

spotify_worldwide_daily_song_ranking.toPandas()
