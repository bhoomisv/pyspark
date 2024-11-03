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
employees_df.createOrReplaceTempView("employees_table")

result_df = spark.sql("""
    SELECT department, SUM(bonus_amount) AS total_bonus FROM (
        SELECT
            department,
            CASE
                WHEN department IN ('Sales', 'Marketing') AND performance_score > 80 THEN performance_score * 0.2
                WHEN performance_score > 70 THEN performance_score * 0.15
                ELSE 0
            END AS bonus_amount
        FROM employees_table
    )
    GROUP BY department
""")

result_df.show()
