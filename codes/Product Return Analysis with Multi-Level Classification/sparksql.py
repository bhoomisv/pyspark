from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder.appName("my code practice").getOrCreate()
products = [
("Laptop", "Electronics", 120, 45),
("Smartphone", "Electronics", 80, 60),
("Tablet", "Electronics", 50, 72),
("Headphones", "Accessories", 110, 47),
("Shoes", "Clothing", 90, 55),
("Jacket", "Clothing", 30, 80),
("TV", "Electronics", 150, 40),
("Watch", "Accessories", 60, 65),
("Pants", "Clothing", 25, 75),
("Camera", "Electronics", 95, 58)
]
products_df = spark.createDataFrame(products, ["product_name", "category", "return_count",
"satisfaction_score"])


products_df.createOrReplaceTempView("products_table")

result_df = spark.sql("""
SELECT
  category,
  return_rate_classification,
  COUNT(*) AS product_count
FROM
  (
    SELECT
      product_name,
      category,
      CASE
        WHEN return_count > 100 AND satisfaction_score < 50 THEN 'High Return Rate'
        WHEN return_count BETWEEN 50 AND 100 AND satisfaction_score BETWEEN 50 AND 70 THEN 'Moderate Return Rate'
        ELSE 'Low Return Rate'
      END AS return_rate_classification
    FROM
      products_table
  )
GROUP BY
  category,
  return_rate_classification;
""")

result_df.show()
