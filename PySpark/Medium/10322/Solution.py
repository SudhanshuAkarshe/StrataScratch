# Import your libraries
import pyspark
from pyspark.sql.functions import min, col, datediff, dayofmonth

# Start writing code
df = amazon_transactions.groupBy('user_id') \
                    .agg(min(col('created_at')).alias('first')) 
                    
df = df.join(amazon_transactions, on ='user_id') \
                    .withColumn('diff', dayofmonth(col('created_at')) - dayofmonth(col('first'))) \
                    .filter((col('diff')>=1) & (col('diff') <=7)) \
                    .select('user_id').distinct()
                    
#.withColumn('date diff',datediff(col('first'), 'created_at'))
                    

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()