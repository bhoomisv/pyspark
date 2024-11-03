'''
Given a DataFrame of sales agents with their total sales amounts, calculate the performance statu
based on sales thresholds: “Excellent” if sales are above 50,000, “Good” if between 25,000 and
50,000, and “Needs Improvement” if below 25,000. Capitalize each agent's name, and show total
sales aggregated by performance status.
'''

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder.appName("Sales Performance").getOrCreate()
sales = [
("karthik", 60000),
("neha", 48000),
("priya", 30000),
("mohan", 24000),
("ajay", 52000),
("vijay", 45000),
("veer", 70000),
("aatish", 23000),
("animesh", 15000),
("nishad", 8000),
("varun", 29000),
("aadil", 32000)
]
sales_df = spark.createDataFrame(sales, ["name", "total_sales"])


sales_df1 = sales_df.withColumn("performance_status", when(col("total_sales") > 50000, "Excellent") \
                                                          .when((col("total_sales") >= 25000) & (col("total_sales") <= 50000), "Good") \
                                                          .otherwise("Needs Improvement")).withColumn("name", F.initcap(col("name")))

result_df = sales_df1.groupBy("performance_status").agg(sum("total_sales").alias("total_sales"))

result_df.show()
