# The election is conducted in a city and everyone can vote for one or more candidates, or choose not to vote at all. Each person has 1 vote so if they vote for multiple candidates, their vote gets equally split across these candidates. For example, if a person votes for 2 candidates, these candidates receive an equivalent of 0.5 vote each. Some voters have chosen not to vote, which explains the blank entries in the dataset.
                                                                                                                                                                                                                                                                                                                                                                                        
# Find out who got the most votes and won the election. Output the name of the candidate or multiple names in case of a tie.
# To avoid issues with a floating-point error you can round the number of votes received by a candidate to 3 decimal places.

# Import your libraries
import pyspark.sql.functions as F

# Start writing code
voting_results = voting_results.where(voting_results.candidate != "")

vote_counts = voting_results.groupBy("voter").agg(F.count("*").alias("num_votes"))

voting_results = voting_results.join(vote_counts, on="voter")

voting_results = voting_results.withColumn("score", 1 / F.col("num_votes"))

voting_results = voting_results.groupBy("candidate").agg(F.sum("score").alias("score"))

max_votes = voting_results.agg(F.max(F.col('score'))).collect()[0][0]

voting_results = voting_results.where(F.col('score') == F.lit(max_votes)).select('candidate')

# To validate your solution, convert your final pySpark df to a pandas df
voting_results.toPandas()

