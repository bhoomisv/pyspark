
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum, when, col, max, avg, min, count, Window
spark = SparkSession.builder.appName("my code practice").getOrCreate()

students = [
("Student1", 70, 45, 60, 65, 75),
("Student2", 80, 55, 58, 62, 67),
("Student3", 65, 30, 45, 70, 55),
("Student4", 90, 85, 80, 78, 76),
("Student5", 72, 40, 50, 48, 52),
("Student6", 88, 60, 72, 70, 68),
("Student7", 74, 48, 62, 66, 70),
("Student8", 82, 56, 64, 60, 66),
("Student9", 78, 50, 48, 58, 55),
("Student10", 68, 35, 42, 52, 45)
]
students_df = spark.createDataFrame(students, ["student_id", "attendance_percentage","math_score", "science_score", "english_score", "history_score"])
students_df.createOrReplaceTempView("students_table")

result_df = spark.sql("""
WITH StudentsWithAvgScore AS (
  SELECT *,
         (math_score + science_score + english_score + history_score) / 4 AS average_score
  FROM students_table
)
SELECT *,
       CASE
           WHEN attendance_percentage < 75 AND average_score < 50 THEN 'At-Risk'
           WHEN attendance_percentage BETWEEN 75 AND 85 THEN 'Moderate Risk'
           ELSE 'Low Risk'
       END AS risk_level
FROM StudentsWithAvgScore;
""")
result_df.show()
result_df1 = spark.sql("""
WITH StudentsWithAvgScore AS (
  SELECT *,
         (math_score + science_score + english_score + history_score) / 4 AS average_score
  FROM students_table
)
SELECT risk_level, COUNT(*) AS student_count
FROM (
  SELECT *,
         CASE
             WHEN attendance_percentage < 75 AND average_score < 50 THEN 'At-Risk'
             WHEN attendance_percentage BETWEEN 75 AND 85 THEN 'Moderate Risk'
             ELSE 'Low Risk'
         END AS risk_level
  FROM StudentsWithAvgScore
)
GROUP BY risk_level;
""")
result_df1.show()
result_df2 = spark.sql("""
WITH StudentsWithAvgScore AS (
  SELECT *,
         (math_score + science_score + english_score + history_score) / 4 AS average_score
  FROM students_table
)

SELECT AVG(average_score) AS avg_score
FROM StudentsWithAvgScore
WHERE risk_level = 'At-Risk';
""")
result_df2.show()
result_df3 = spark.sql("""
WITH StudentsWithAvgScore AS (
  SELECT *,
         (math_score + science_score + english_score + history_score) / 4 AS average_score
  FROM students_table
)

SELECT student_id
FROM StudentsWithAvgScore
WHERE risk_level = 'Moderate Risk'
GROUP BY student_table
HAVING SUM(CASE WHEN math_score > 70 OR science_score > 70 OR english_score > 70 OR history_score > 70 THEN 1 ELSE 0 END) >= 3;
""")
result_df3.show()
