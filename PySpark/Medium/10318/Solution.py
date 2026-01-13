# Import your libraries
import pyspark

# Start writing code
from pyspark.sql.functions import col, count

d1 = car_launches.groupBy('company_name') \
                    .pivot('year') \
                    .agg(count('product_name').alias('count')) \
                    .withColumn('net_products', col('2020') - col('2019')) \
                    .select('company_name', 'net_products')
                    
d1.toPandas()