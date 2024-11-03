
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
employees_df.createOrReplaceTempView("employees_table")

# Execute the SQL query
result_df = spark.sql("""
    SELECT overtime_status, COUNT(*) AS employee_count FROM (
        SELECT 
            INITCAP(name) AS name,
            hours_worked,
            CASE 
                WHEN hours_worked > 60 THEN 'Excessive Overtime'
                WHEN hours_worked >= 45 AND hours_worked <= 60 THEN 'Standard Overtime'
                ELSE 'No Overtime'
            END AS overtime_status
        FROM employees_table
    )
    GROUP BY overtime_status
""")

result_df.show()
