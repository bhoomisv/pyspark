from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder.appName("Student Grade Classification").getOrCreate()

students = [
("karthik", 95),
("neha", 82),
("priya", 74),
("mohan", 91),
("ajay", 67),
("vijay", 80),
("veer", 85),
("aatish", 72),
("animesh", 90),
("nishad", 60)
]
students_df = spark.createDataFrame(students, ["name", "score"])
students_df.createOrReplaceTempView("students_table")

result_df = spark.sql("""
    SELECT grade, COUNT(*) AS student_count FROM (
        SELECT 
            INITCAP(name) AS name,
            score,
            CASE 
                WHEN score >= 90 THEN 'Excellent'
                WHEN score >= 75 AND score <= 89 THEN 'Good'
                ELSE 'Needs Improvement'
            END AS grade
        FROM students_table
    )
    GROUP BY grade
""")
result_df.show()

