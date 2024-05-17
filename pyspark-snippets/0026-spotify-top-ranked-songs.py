# Find songs that have ranked in the top position. Output the track name and the number of times it ranked at the top. 
# Sort your records by the number of times the song was in the top position in descending order.

import pyspark

spotify_worldwide_daily_song_ranking = spotify_worldwide_daily_song_ranking.filter(spotify_worldwide_daily_song_ranking.position == 1).groupby('trackname').count()

spotify_worldwide_daily_song_ranking.toPandas()
