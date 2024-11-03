'''Question 3: Project Allocation and Workload Analysis
Given a DataFrame with project allocation data for multiple employees, determine each employee's
workload level based on their hours worked in a month across various projects. Categorize
employees as “Overloaded” if they work more than 200 hours, “Balanced” if between 100-200 hours,
and “Underutilized” if below 100 hours. Capitalize each employee’s name, and show the aggregated
workload status count by category.
'''
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder.appName("Workload Analysis").getOrCreate()

workload = [
("karthik", "ProjectA", 120),
("karthik", "ProjectB", 100),
("neha", "ProjectC", 80),
("neha", "ProjectD", 30),
("priya", "ProjectE", 110),
("mohan", "ProjectF", 40),
("ajay", "ProjectG", 70),
("vijay", "ProjectH", 150),
("veer", "ProjectI", 190),
("aatish", "ProjectJ", 60),
("animesh", "ProjectK", 95),
("nishad", "ProjectL", 210),
("varun", "ProjectM", 50),
("aadil", "ProjectN", 90)
]
workload_df = spark.createDataFrame(workload, ["name", "project", "hours"])
workload_df = workload_df.groupBy("name").agg(sum("hours").alias("total_hours"))
workload_df = workload_df.withColumn("workload_status", when(col("total_hours") > 200, "Overloaded") \
                                                                 .when((col("total_hours") >= 100) & (col("total_hours") <= 200), "Balanced") \
                                                                 .otherwise("Underutilized")) \
                                                 .withColumn("name", F.initcap(col("name")))
result_df = workload_df.groupBy("workload_status").count().withColumnRenamed("count", "employee_count")

result_df.show()
