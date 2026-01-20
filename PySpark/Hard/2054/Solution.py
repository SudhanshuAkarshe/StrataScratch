# Import your libraries
import pyspark
from pyspark.sql.functions import col,datediff,lag, when, lead, sum,desc, row_number,rank
from pyspark.sql.window import Window

w = Window.partitionBy('user_id').orderBy(col('record_date'))
# Start writing code
df = sf_events.withColumn('lagDiff', datediff(col('record_date'), lag(col('record_date')).over(w))) \
            .withColumn('leadDiff', datediff(col('record_date'), lead(col('record_date')).over(w))) \
            .filter((col('leadDiff')>=-1) & (col('lagDiff')<=1) & (col('lagDiff')!=0)) \
            .select('user_id')
        
    


#df = sf_events.withColumn('streak', when(datediff(col('record_date'), lag(col('record_date')).over(w))==1,rank().over(w)).otherwise(0)) \
#                .withColumn('cons', sum(col('streak')).over(w))

# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()