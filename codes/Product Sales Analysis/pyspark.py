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

product_sales_df = product_sales_df.withColumn("classification", when((col("total_sales") > 200000) & (col("discount") < 10), "Top Seller").when((col("total_sales") >= 100000) & (col("total_sales") <= 200000), "Moderate Seller").otherwise("Low Seller"))
product_counts = product_sales_df.groupBy("classification").count()
product_counts.show()

max_top_seller_sales = product_sales_df.filter(col("classification") == "Top Seller").agg(max("total_sales").alias("max_sales"))
min_moderate_seller_discount = product_sales_df.filter(col("classification") == "Moderate Seller").agg(min("discount").alias("min_discount"))
max_top_seller_sales.show()
min_moderate_seller_discount.show()

low_seller_high_discount = product_sales_df.filter((col("classification") == "Low Seller") & (col("total_sales") < 50000) & (col("discount") > 15))
low_seller_high_discount.show()
