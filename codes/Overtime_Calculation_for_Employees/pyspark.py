'''5. Overtime Calculation for Employees
Determine whether an employee has "Excessive Overtime" if their weekly hours exceed 60,
"Standard Overtime" if between 45-60 hours, and "No Overtime" if below 45 hours. Capitalize each
name and group by overtime status.
'''
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder().appName("OvertimeAnalysis").getOrCreate() 

employees = [
("karthik", 62),
("neha", 50),
("priya", 30),
("mohan", 65),
  ("ajay", 40),
("vijay", 47),
("veer", 55),
("aatish", 30),
("animesh", 75),
("nishad", 60)
]
employees_df = spark.createDataFrame(employees, ["name", "hours_worked"])
employees_df = employees_df.withColumn("overtime_status", when(col("hours_worked") > 60, "Excessive Overtime") \
                                                                 .when((col("hours_worked") >= 45) & (col("hours_worked") <= 60), "Standard Overtime") \
                                                                 .otherwise("No Overtime")).withColumn("name", F.initcap(col("name")))
result_df = employees_df.groupBy("overtime_status").count().withColumnRenamed("count", "employee_count")
result_df.show()




