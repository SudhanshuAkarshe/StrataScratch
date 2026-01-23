# Import your libraries
import pyspark
from pyspark.sql.functions import sum, col


# Start writing code
df = ms_user_dimension.join(ms_acc_dimension, on = 'acc_id', how = 'left') 
df = df.join(ms_download_facts, on = 'user_id') \
        .groupBy('date') \
        .pivot('paying_customer') \
        .agg(sum('downloads')) \
        .filter(col('no')>col('yes')) \
        .orderBy('date')

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()