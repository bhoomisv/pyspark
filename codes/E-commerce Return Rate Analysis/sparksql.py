from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum, when, col, max, avg, min
spark = SparkSession.builder.appName("my code practice").getOrCreate()

ecommerce_return = [
("Product1", 75, 25),
("Product2", 40, 15),
("Product3", 30, 5),
("Product4", 60, 18),
("Product5", 100, 30),
("Product6", 45, 10),
("Product7", 80, 22),
("Product8", 35, 8),
("Product9", 25, 3),
("Product10", 90, 12)
]
ecommerce_return_df = spark.createDataFrame(ecommerce_return, ["product_name", "sale_price","return_rate"])
ecommerce_return_df.createOrReplaceTempView("ecommerce_return_table")
result_df = spark.sql("""
SELECT
  product_name,
  sale_price,
  return_rate,
  CASE
    WHEN return_rate > 20 THEN 'High Return'
    WHEN return_rate BETWEEN 10 AND 20 THEN 'Medium Return'
    ELSE 'Low Return'
  END AS return_category
FROM
  ecommerce_return_table;
""")
result_df.show()
result_df1 = spark.sql("""
SELECT
  return_category,
  COUNT(*) AS product_count
FROM
  (
    SELECT
      product_name,
      sale_price,
      return_rate,
      CASE
        WHEN return_rate > 20 THEN 'High Return'
        WHEN return_rate BETWEEN 10 AND 20 THEN 'Medium Return'
        ELSE 'Low Return'
      END AS return_category
    FROM
      ecommerce_return_table
  )
GROUP BY
  return_category;
""")
result_df1.show()
result_df2 = spark.sql("""
SELECT
  AVG(sale_price) AS avg_price
FROM
  (
    SELECT
      product_name,
      sale_price,
      return_rate,
      CASE
        WHEN return_rate > 20 THEN 'High Return'
        WHEN return_rate BETWEEN 10 AND 20 THEN 'Medium Return'
        ELSE 'Low Return'
      END AS return_category
    FROM
      ecommerce_return_table
  )
WHERE
  return_category = 'High Return';
""")
result_df2.show()
result_df3 = spark.sql("""
SELECT
  MAX(return_rate) AS max_return_rate
FROM
  (
    SELECT
      product_name,
      sale_price,
      return_rate,
      CASE
        WHEN return_rate > 20 THEN 'High Return'
        WHEN return_rate BETWEEN 10 AND 20 THEN 'Medium Return'
        ELSE 'Low Return'
      END AS return_category
    FROM
      ecommerce_return_table
  )
WHERE
  return_category = 'Medium Return';
""")
result_df3.show()
result_df4 = spark.sql("""
SELECT
  *
FROM
  (
    SELECT
      product_name,
      sale_price,
      return_rate,
      CASE
        WHEN return_rate > 20 THEN 'High Return'
        WHEN return_rate BETWEEN 10 AND 20 THEN 'Medium Return'
        ELSE 'Low Return'
      END AS return_category
    FROM
      ecommerce_return_table
  )
WHERE
  return_category = 'Low Return' AND sale_price < 50 AND return_rate < 5;
""")
result_df4.show()
