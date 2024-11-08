from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder.appName("my code practice").getOrCreate()
loan_applicants = [
("karthik", 60000, 120000, 590),
("neha", 90000, 180000, 610),
("priya", 50000, 75000, 680),
("mohan", 120000, 240000, 560),
("ajay", 45000, 60000, 620),
("vijay", 100000, 100000, 700),
("veer", 30000, 90000, 580),
("aatish", 85000, 85000, 710),
("animesh", 50000, 100000, 650),
("nishad", 75000, 200000, 540)
]
loan_applicants_df = spark.createDataFrame(loan_applicants, ["name", "income", "loan_amount",
"credit_score"])


loan_applicants_df.createOrReplaceTempView("loan_applicants_table")

result_df = spark.sql("""
SELECT
  name,
  income,
  loan_amount,
  credit_score,
  CASE
    WHEN loan_amount > 2 * income AND credit_score < 600 THEN 'High Risk'
    WHEN loan_amount BETWEEN income AND 2 * income AND credit_score BETWEEN 600 AND 700 THEN 'Moderate Risk'
    ELSE 'Low Risk'
  END AS risk_level
FROM
  loan_applicants_table;
""")
result_df.show()

result_df1 = spark.sql("""
SELECT
  risk_level,
  COUNT(*) AS count
FROM
  (
    SELECT
      name,
      income,
      loan_amount,
      credit_score,
      CASE
        WHEN loan_amount > 2 * income AND credit_score < 600 THEN 'High Risk'
        WHEN loan_amount BETWEEN income AND 2 * income AND credit_score BETWEEN 600 AND 700 THEN 'Moderate Risk'
        ELSE 'Low Risk'
      END AS risk_level
    FROM
      loan_applicants_table
  )
GROUP BY
  risk_level;
  
""")
result_df1.show()


result_df2 = spark.sql("""
SELECT
  CASE
    WHEN income < 50000 THEN '<50k'
    WHEN income >= 50000 AND income < 100000 THEN '50-100k'
    ELSE '>100k'
  END AS income_range,
  AVG(loan_amount) AS avg_loan_amount
FROM
  (
    SELECT
      name,
      income,
      loan_amount,
      credit_score,
      CASE
        WHEN loan_amount > 2 * income AND credit_score < 600 THEN 'High Risk'
        WHEN loan_amount BETWEEN income AND 2 * income AND credit_score BETWEEN 600 AND 700 THEN 'Moderate Risk'
        ELSE 'Low Risk'
      END AS risk_level
    FROM
      loan_applicants_table
  )
WHERE
  risk_level = 'High Risk'
GROUP BY
  income_range;
""")
result_df2.show()



result_df3 = spark.sql("""
SELECT
  CASE
    WHEN income < 50000 THEN '<50k'
    WHEN income >= 50000 AND income < 100000 THEN '50-100k'
    ELSE '>100k'
  END AS income_range,
  risk_level,
  AVG(credit_score) AS avg_credit_score
FROM
  (
    SELECT
      name,
      income,
      loan_amount,
      credit_score,
      CASE
        WHEN loan_amount > 2 * income AND credit_score < 600 THEN 'High Risk'
        WHEN loan_amount BETWEEN income AND 2 * income AND credit_score BETWEEN 600 AND 700 THEN 'Moderate Risk'
        ELSE 'Low Risk'
      END AS risk_level
    FROM
      loan_applicants_table
  )
GROUP BY
  income_range,
  risk_level
HAVING
  AVG(credit_score) < 650;
""")
result_df3.show()

