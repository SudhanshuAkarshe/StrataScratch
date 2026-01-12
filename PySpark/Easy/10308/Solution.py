# Import your libraries
import pyspark

# Start writing code
#db_employee
from pyspark.sql import functions as F

df = db_employee.join(db_dept, db_employee['department_id'] == db_dept['id'], how = 'left').select(db_employee['*'], db_dept['department'])

df = df.groupBy('department').max('salary')

df = df.filter((F.col('department') == 'marketing') | (F.col('department') == 'engineering'))


df_pivot = df.groupBy().pivot("department").agg(F.first("max(salary)"))

db_employee = df_pivot.select(F.abs(F.col("marketing") - F.col("engineering")).alias("salary_difference"))


#print(df)
#print(df_pivot)


# To validate your solution, convert your final pySpark df to a pandas df
db_employee.toPandas()