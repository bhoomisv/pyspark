from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder.appName("my code practice").getOrCreate()
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

employees_df = employees_df.withColumn(
    "bonus_percentage",
    when(
        (col("department").isin("Sales", "Marketing")) & (col("performance_score") > 80),
        20
    ).otherwise(
        when(col("performance_score") > 70, 15).otherwise(0)
    )
)
bonus_by_dept = employees_df.groupBy("department").agg(
    (sum("bonus_percentage") / 100 * employees_df.count()).alias("total_bonus")
)

bonus_by_dept.show()
