# Import your libraries
import pyspark
from pyspark.sql.functions import month, col, max, sum,first,desc, row_number
from pyspark.sql.window import Window

w = Window.partitionBy('month').orderBy(col('sales').desc())
# Start writing code
df = online_retail.filter(col('quantity')>0)\
                .withColumn('Month', month(col('invoicedate'))) \
                .withColumn('sales', col('unitprice') * col('quantity')) \
                .groupBy('month','description') \
                .agg( sum('sales').alias('total_paid')) \
                .withColumn('rn', row_number().over(Window.partitionBy('month').orderBy(col('total_paid').desc()))) \
                .filter(col('rn')==1) \
                .select('month','total_paid','description')
              
                

df1 = online_retail.withColumn('Month', month(col('invoicedate'))) \
                .filter(col('Month') ==7)
# To validate your solution, convert your final pySpark df to a pandas df
df.toPandas()