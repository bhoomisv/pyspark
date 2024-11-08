from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum, when, col, max, avg
spark = SparkSession.builder.appName("my code practice").getOrCreate()

employees = [
("karthik", "IT", 110000, 12, 88),
("neha", "Finance", 75000, 8, 70),
("priya", "IT", 50000, 5, 65),
("mohan", "HR", 120000, 15, 92),
("ajay", "IT", 45000, 3, 50),
("vijay", "Finance", 80000,
("veer", "Marketing", 95000, 6, 85),
("aatish", "HR", 100000, 9, 82),
("animesh", "Finance", 105000, 11, 88),
("nishad", "IT", 30000, 2, 55)]
employees_df = spark.createDataFrame(employees , ["name", "department", "salary", "experience", "performance_score"])

employees_df.createOrReplaceTempView("employees_table")
result_df = spark.sql("""
SELECT
  name,
  department,
  salary,
  experience,
  performance_score,
  CASE
    WHEN salary > 100000 AND experience > 10 THEN 'Senior'
    WHEN salary BETWEEN 50000 AND 100000 AND experience BETWEEN 5 AND 10 THEN 'Mid-level'
    ELSE 'Junior'
  END AS salary_band
FROM
  employees_table;
""")
result_df.show()
result_df1 = spark.sql("""
SELECT
  department,
  salary_band,
  COUNT(*) AS employee_count
FROM
  (
    SELECT
      name,
      department,
      salary,
      experience,
      performance_score,
      CASE
        WHEN salary > 100000 AND experience > 10 THEN 'Senior'
        WHEN salary BETWEEN 50000 AND 100000 AND experience BETWEEN 5 AND 10 THEN 'Mid-level'
        ELSE 'Junior'
      END AS salary_band
    FROM
      employees_table
  )
GROUP BY
  department,
  salary_band;
""")
result_df1.show()
result_df2 = spark.sql("""
SELECT
  salary_band,
  AVG(performance_score) AS avg_performance
FROM
  (
    SELECT
      name,
      department,
      salary,
      experience,
      performance_score,
      CASE
        WHEN salary > 100000 AND experience > 10 THEN 'Senior'
        WHEN salary BETWEEN 50000 AND 100000 AND experience BETWEEN 5 AND 10 THEN 'Mid-level'
        ELSE 'Junior'
      END AS salary_band
    FROM
      employees_table
  )
GROUP BY
  salary_band
HAVING
  AVG(performance_score) > 80;
""")
result_df2.show()
result_df3 = spark.sql("""
SELECT
  *
FROM
  (
    SELECT
      name,
      department,
      salary,
      experience,
      performance_score,
      CASE
        WHEN salary > 100000 AND experience > 10 THEN 'Senior'
        WHEN salary BETWEEN 50000 AND 100000 AND experience BETWEEN 5 AND 10 THEN 'Mid-level'
        ELSE 'Junior'
      END AS salary_band
    FROM
      employees_table
  )
WHERE
  salary_band = 'Mid-level' AND performance_score > 85 AND experience > 7;
""")
result_df3.show()
