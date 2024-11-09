from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum, when, col, max, avg, min
spark = SparkSession.builder.appName("my code practice").getOrCreate()

customer_loyalty = [
("Customer1", 25, 700),
("Customer2", 15, 400),
("Customer3", 5, 50),
("Customer4", 18, 450),
("Customer5", 22, 600),
("Customer6", 2, 80),
("Customer7", 12, 300),
("Customer8", 6, 150),
("Customer9", 10, 200),
("Customer10", 1, 90)
]
customer_loyalty_df = spark.createDataFrame(customer_loyalty, ["customer_name", "purchase_frequency", "average_spending"])

customer_loyalty_df.createOrReplaceTempView("customer_loyalty_table")

result_df = spark.sql("""
SELECT
  customer_name,
  purchase_frequency,
  average_spending,
  CASE
    WHEN purchase_frequency > 20 AND average_spending > 500 THEN 'Highly Loyal'
    WHEN purchase_frequency BETWEEN 10 AND 20 THEN 'Moderately Loyal'
    ELSE 'Low Loyalty'
  END AS loyalty_level
FROM
  customer_loyalty_table;
""")
result_df.show()
result_df1 = spark.sql("""
SELECT
  loyalty_level,
  COUNT(*) AS customer_count
FROM
  (
    SELECT
      customer_name,
      purchase_frequency,
      average_spending,
      CASE
        WHEN purchase_frequency > 20 AND average_spending > 500 THEN 'Highly Loyal'
        WHEN purchase_frequency BETWEEN 10 AND 20 THEN 'Moderately Loyal'
        ELSE 'Low Loyalty'
      END AS loyalty_level
    FROM
      customer_loyalty_table
  )
GROUP BY
  loyalty_level;
""")
result_df1.show()
result_df2 = spark.sql("""
SELECT
  AVG(average_spending) AS avg_spending
FROM
  (
    SELECT
      customer_name,
      purchase_frequency,
      average_spending,
      CASE
        WHEN purchase_frequency > 20 AND average_spending > 500 THEN 'Highly Loyal'
        WHEN purchase_frequency BETWEEN 10 AND 20 THEN 'Moderately Loyal'
        ELSE 'Low Loyalty'
      END AS loyalty_level
    FROM
      customer_loyalty_table
  )
WHERE
  loyalty_level = 'Highly Loyal';
""")
result_df2.show()
result_df3 = spark.sql("""
SELECT
  MIN(average_spending) AS min_spending
FROM
  (
    SELECT
      customer_name,
      purchase_frequency,
      average_spending,
      CASE
        WHEN purchase_frequency > 20 AND average_spending > 500 THEN 'Highly Loyal'
        WHEN purchase_frequency BETWEEN 10 AND 20 THEN 'Moderately Loyal'
        ELSE 'Low Loyalty'
      END AS loyalty_level
    FROM
      customer_loyalty_table
  )
WHERE
  loyalty_level = 'Moderately Loyal';
""")
result_df3.show()
result_df4 = spark.sql("""
SELECT
  *
FROM
  (
    SELECT
      customer_name,
      purchase_frequency,
      average_spending,
      CASE
        WHEN purchase_frequency > 20 AND average_spending > 500 THEN 'Highly Loyal'
        WHEN purchase_frequency BETWEEN 10 AND 20 THEN 'Moderately Loyal'
        ELSE 'Low Loyalty'
      END AS loyalty_level
    FROM
      customer_loyalty_table
  )
WHERE
  loyalty_level = 'Low Loyalty' AND average_spending < 100 AND purchase_frequency < 5;
""")
result_df4.show()



