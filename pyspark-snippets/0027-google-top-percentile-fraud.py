# ABC Corp is a mid-sized insurer in the US and in the recent past their fraudulent claims have increased significantly for their personal auto insurance portfolio. They have developed a ML based predictive model to identify propensity of fraudulent claims. Now, they assign highly experienced claim adjusters for top 5 percentile of claims identified by the model.
# Your objective is to identify the top 5 percentile of claims from each state. Your output should be policy number, state, claim cost, and fraud score.

import pyspark.sql.functions as F
from pyspark.sql import Window


result = fraud_score \
    .withColumn('pcnt_rank', F.percent_rank().over(Window.partitionBy('state').orderBy('fraud_score'))) \
    .where(F.col('pcnt_rank') >= 0.95) \
    .select('policy_num', 'state', 'claim_cost', 'fraud_score') \
    .toPandas()
