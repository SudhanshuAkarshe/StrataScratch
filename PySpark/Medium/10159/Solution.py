# Import your libraries
import pyspark
from pyspark.sql.functions import col, dense_rank, desc,sum
from pyspark.sql.window import Window


airbnb_contacts = airbnb_contacts \
                    .groupBy('id_guest') \
                    .agg(sum(col('n_messages')).alias('sum_n_messages')) \
                    .withColumn('Ranking', dense_rank().over(Window.orderBy(desc('sum_n_messages')))) 

# Start writing code
#airbnb_contacts = airbnb_contacts.groupBy(col('id_guest'),col('id_host')).count('*').select('*')

# To validate your solution, convert your final pySpark df to a pandas df
airbnb_contacts.toPandas()