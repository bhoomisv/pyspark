from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum, when, col, max, avg, min
spark = SparkSession.builder.appName("my code practice").getOrCreate()
employee_productivity = [
("Emp1", 85, 6),
("Emp2", 75, 4),
("Emp3", 40, 1),
("Emp4", 78, 5),
("Emp5", 90, 7),
("Emp6", 55, 3),
("Emp7", 80, 5),
("Emp8", 42, 2),
("Emp9", 30, 1),
("Emp10", 68, 4)
]
employee_productivity_df = spark.createDataFrame(employee_productivity, ["employee_id", "productivity_score", "project_count"])
employee_productivity_df.createOrReplaceTempView("employee_productivity_table")

result_df = spark.sql("""
SELECT
  employee_id,
  productivity_score,
  project_count,
  CASE
    WHEN productivity_score > 80 AND project_count > 5 THEN 'High Performer'
    WHEN productivity_score BETWEEN 60 AND 80 THEN 'Average Performer'
    ELSE 'Low Performer'
  END AS performance_level
FROM
  employee_productivity_table;
""")
result_df.show()

result_df1 = spark.sql("""
SELECT
  performance_level,
  COUNT(*) AS employee_count
FROM
  (
    SELECT
      employee_id,
      productivity_score,
      project_count,
      CASE
        WHEN productivity_score > 80 AND project_count > 5 THEN 'High Performer'
        WHEN productivity_score BETWEEN 60 AND 80 THEN 'Average Performer'
        ELSE 'Low Performer'
      END AS performance_level
    FROM
      employee_productivity_table
  )
GROUP BY
  performance_level;
""")
result_df1.show()

result_df2= spark.sql("""
SELECT
  AVG(productivity_score) AS avg_score
FROM
  (
    SELECT
      employee_id,
      productivity_score,
      project_count,
      CASE
        WHEN productivity_score > 80 AND project_count > 5 THEN 'High Performer'
        WHEN productivity_score BETWEEN 60 AND 80 THEN 'Average Performer'
        ELSE 'Low Performer'
      END AS performance_level
    FROM
      employee_productivity_table
  )
WHERE
  performance_level = 'High Performer';
""")
result_df2.show()

result_df3 = spark.sql("""
SELECT
  MIN(productivity_score) AS min_score
FROM
  (
    SELECT
      employee_id,
      productivity_score,
      project_count,
      CASE
        WHEN productivity_score > 80 AND project_count > 5 THEN 'High Performer'
        WHEN productivity_score BETWEEN 60 AND 80 THEN 'Average Performer'
        ELSE 'Low Performer'
      END AS performance_level
    FROM
      employee_productivity_table
  )
WHERE
  performance_level = 'Average Performer';
""")
result_df3.show()

result_df4 = spark.sql("""
SELECT
  *
FROM
  (
    SELECT
      employee_id,
      productivity_score,
      project_count,
      CASE
        WHEN productivity_score > 80 AND project_count > 5 THEN 'High Performer'
        WHEN productivity_score BETWEEN 60 AND 80 THEN 'Average Performer'
        ELSE 'Low Performer'
      END AS performance_level
    FROM
      employee_productivity_table
  )
WHERE
  performance_level = 'Low Performer' AND productivity_score < 50 AND project_count < 2;
""")
result_df4.show()
