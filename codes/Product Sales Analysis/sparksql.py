from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum, when, col, max, min, avg
spark = SparkSession.builder.appName("my code practice").getOrCreate()

product_sales = [("Product1", 250000, 5),
("Product2", 150000, 8),
("Product3", 50000, 20),
("Product4", 120000, 10),
("Product5", 300000, 7),
("Product6", 60000, 18),
("Product7", 180000, 9),
("Product8", 45000, 25),
("Product9", 70000, 15),
("Product10", 10000, 30)]

product_sales_df = spark.createDataFrame(product_sales, ["product_name", "total_sales", "discount"])

product_sales_df.createOrReplaceTempView("product_sales_table")
result_df = spark.sql("""
SELECT
  product_name,
  total_sales,
  discount,
  CASE
    WHEN total_sales > 200000 AND discount < 10 THEN 'Top Seller'
    WHEN total_sales BETWEEN 100000 AND 200000 THEN 'Moderate Seller'
    ELSE 'Low Seller'
  END AS classification
FROM
  product_sales_table;
""")
result_df.show()
result_df1 = spark.sql("""
SELECT
  classification,
  COUNT(*) AS product_count
FROM
  (
    SELECT
      product_name,
      total_sales,
      discount,
      CASE
        WHEN total_sales > 200000 AND discount < 10 THEN 'Top Seller'
        WHEN total_sales BETWEEN 100000 AND 200000 THEN 'Moderate Seller'
        ELSE 'Low Seller'
      END AS classification
    FROM
      product_sales_table
  )
GROUP BY
  classification;
""")
result_df1.show()
result_df2 = spark.sql("""
SELECT
  MAX(total_sales) AS max_sales
FROM
  product_sales_table
WHERE
  classification = 'Top Seller';
""")
result_df2.show()
result_df3 = spark.sql("""
SELECT
  MIN(discount) AS min_discount
FROM
  product_sales_table
WHERE
  classification = 'Moderate Seller';
""")
result_df3.show()
result_df4 = spark.sql("""
SELECT
  *
FROM
  (
    SELECT
      product_name,
      total_sales,
      discount,
      CASE
        WHEN total_sales > 200000 AND discount < 10 THEN 'Top Seller'
        WHEN total_sales BETWEEN 100000 AND 200000 THEN 'Moderate Seller'
        ELSE 'Low Seller'
      END AS classification
    FROM
      product_sales_table
  )
WHERE
  classification = 'Low Seller' AND total_sales < 50000 AND discount > 15;
""")
result_df4.show()

