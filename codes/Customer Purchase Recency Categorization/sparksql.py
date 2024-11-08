from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder.appName("my code practice").getOrCreate()


customer_purchases = [
("karthik", "Premium", 50, 5000),
("neha", "Standard", 10, 2000),
("priya", "Premium", 65, 8000),
("mohan", "Basic", 90, 1200),
("ajay", "Standard", 25, 3500),
("vijay", "Premium", 15, 7000),
("veer", "Basic", 75, 1500),
("aatish", "Standard", 45, 3000),
("animesh", "Premium", 20, 9000),
("nishad", "Basic", 80, 1100)
]
customer_purchases_df = spark.createDataFrame(customer_purchases, ["name", "membership", "days_since_last_purchase", "total_purchase_amount"])
customer_purchases_df.createOrReplaceTempView("customer_purchases_table")
result_df = spark.sql("""
SELECT
  name,
  membership,
  days_since_last_purchase,
  total_purchase_amount,
  CASE
    WHEN days_since_last_purchase <= 30 THEN 'Frequent'
    WHEN days_since_last_purchase <= 60 THEN 'Occasional'
    ELSE 'Rare'
  END AS purchase_recency
FROM
  customer_purchases_table;
""")

result_df.show()

result_df1 = spark.sql("""
SELECT
  membership,
  purchase_recency,
  COUNT(*) AS customer_count
FROM
  (
    SELECT
      name,
      membership,
      days_since_last_purchase,
      total_purchase_amount,
      CASE
        WHEN days_since_last_purchase <= 30 THEN 'Frequent'
        WHEN days_since_last_purchase <= 60 THEN 'Occasional'
        ELSE 'Rare'
      END AS purchase_recency
    FROM
      customer_purchases_table
  )
GROUP BY
  membership,
  purchase_recency;
""")

result_df1.show()

result_df2 = spark.sql("""
SELECT
  AVG(total_purchase_amount) AS avg_purchase_amount
FROM
  (
    SELECT
      name,
      membership,
      days_since_last_purchase,
      total_purchase_amount,
      CASE
        WHEN days_since_last_purchase <= 30 THEN 'Frequent'
        WHEN days_since_last_purchase <= 60 THEN 'Occasional'
        ELSE 'Rare'
      END AS purchase_recency
    FROM
      customer_purchases_table
  )
WHERE
  purchase_recency = 'Frequent' AND membership = 'Premium';
""")

result_df2.show()

result_df3 = spark.sql("""
SELECT
  membership,
  MIN(total_purchase_amount) AS min_purchase_amount
FROM
  (
    SELECT
      name,
      membership,
      days_since_last_purchase,
      total_purchase_amount,
      CASE
        WHEN days_since_last_purchase <= 30 THEN 'Frequent'
        WHEN days_since_last_purchase <= 60 THEN 'Occasional'
        ELSE 'Rare'
      END AS purchase_recency
    FROM
      customer_purchases_table
  )
WHERE
  purchase_recency = 'Rare'
GROUP BY
  membership;
""")

result_df3.show()





