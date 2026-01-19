# Import your libraries
import pyspark
from pyspark.sql.window import Window
from pyspark.sql.functions import col,max, first
# Start writing code


window_spec = Window.partitionBy(col('id')).orderBy(col('salary').desc())
ms_employee_salary = ms_employee_salary.withColumn('current_salary', max(col('salary')).over(window_spec)).drop('salary').dropDuplicates(['id'])


# To validate your solution, convert your final pySpark df to a pandas df
ms_employee_salary.toPandas()