
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum, when, col, max, avg, min, count, Window
spark = SparkSession.builder.appName("my code practice").getOrCreate()

transactions = [
("Account1", "2024-11-01", 12000, 6, "Savings"),
("Account2", "2024-11-01", 8000, 3, "Current"),
("Account3", "2024-11-02", 2000, 1, "Savings"),
("Account4", "2024-11-02", 15000, 7, "Savings"),
("Account5", "2024-11-03", 9000, 4, "Current"),
("Account6", "2024-11-03", 3000, 1, "Current"),
("Account7", "2024-11-04", 13000, 5, "Savings"),
("Account8", "2024-11-04", 6000, 2, "Current"),
("Account9", "2024-11-05", 20000, 8, "Savings"),
("Account10", "2024-11-05", 7000, 3, "Savings")
]
transactions_df = spark.createDataFrame(transactions, ["account_id", "transaction_date", "amount", "frequency", "account_type"])
transactions_df.createOrReplaceTempView("transactions_table")

result_df = spark.sql("""
SELECT *,
       CASE
           WHEN amount > 10000 AND frequency > 5 THEN 'High Risk'
           WHEN amount BETWEEN 5000 AND 10000 AND frequency BETWEEN 2 AND 5 THEN 'Moderate Risk'
           ELSE 'Low Risk'
       END AS risk_level
FROM transactions_table;
""")
result_df.show()
result_df1 = spark.sql("""
SELECT risk_level, COUNT(*) AS transaction_count
FROM (
  SELECT *,
         CASE
             WHEN amount > 10000 AND frequency > 5 THEN 'High Risk'
             WHEN amount BETWEEN 5000 AND 10000 AND frequency BETWEEN 2 AND 5 THEN 'Moderate Risk'
             ELSE 'Low Risk'
         END AS risk_level
  FROM transactions_table
)
GROUP BY risk_level;
""")
result_df1.show()
result_df2 = spark.sql("""
WITH HighRiskAccounts AS (
  SELECT
    account_id,
    SUM(amount) OVER (PARTITION BY account_id) AS total_amount
  FROM transactions_table
  WHERE risk_level = 'High Risk'
)
SELECT * FROM HighRiskAccounts;
""")
result_df2.show()
result_df3 = spark.sql("""
SELECT *
FROM transactions_table
WHERE risk_level = 'Moderate Risk' AND account_type = 'Savings' AND amount > 7500;

""")
result_df3.show()
