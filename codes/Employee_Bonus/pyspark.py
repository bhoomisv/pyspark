from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder.appName("Employee Bonus Calculation").getOrCreate()

employees = [
("karthik", "Sales", 85),
("neha", "Marketing", 78),
("priya", "IT", 90),
("mohan", "Finance", 65),
("ajay", "Sales", 55),
("vijay", "Marketing", 82),
("veer", "HR", 72),
("aatish", "Sales", 88),
("animesh", "Finance", 95),
("nishad", "IT", 60)
]
employees_df = spark.createDataFrame(employees, ["name", "department", "performance_score"])
employees_df = employees_df.withColumn("bonus_percentage", when((col("department") == "Sales") | (col("department") == "Marketing"), \
                                                            when(col("performance_score") > 80), 0.2.otherwise(0)) \
                                                                 .otherwise(when(col("performance_score") > 70, 0.15).otherwise(0)))
employees_df = employees_df.withColumn("bonus_amount", (col("bonus_percentage") * col("performance_score")))

result_df = employees_dfgroupBy("department").agg(sum("bonus_amount").as("total_bonus"))

result_df.show(truncate=False)
