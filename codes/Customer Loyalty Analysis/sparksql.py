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



customer_loyalty_df = customer_loyalty_df.withColumn("loyalty_level",when((col("purchase_frequency") > 20) & (col("average_spending") > 500), "Highly Loyal").when((col("purchase_frequency") >= 10) & (col("purchase_frequency") <= 20), "Moderately Loyal").otherwise("Low Loyalty"))


loyalty_level_counts = customer_loyalty_df.groupBy("loyalty_level").count()
loyalty_level_counts.show()


avg_highly_loyal_spending = customer_loyalty_df.filter(col("loyalty_level") == "Highly Loyal").agg(avg("average_spending").alias("avg_spending"))
avg_highly_loyal_spending.show()


min_moderately_loyal_spending = customer_loyalty_df.filter(col("loyalty_level") == "Moderately Loyal").agg(min("average_spending").alias("min_spending"))
min_moderately_loyal_spending.show()

low_loyalty_low_spending_freq = customer_loyalty_df.filter((col("loyalty_level") == "Low Loyalty") & (col("average_spending") < 100) & (col("purchase_frequency") < 5))
low_loyalty_low_spending_freq.show()
