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
ecommerce_return_df = spark.createDataFrame(ecommerce_return, ["product_name", "sale_price",
"return_rate"])

ecommerce_return_df = ecommerce_return_df.withColumn("return_category", when(col("return_rate") > 20, "High Return").when((col("return_rate") >= 10) & (col("return_rate") <= 20), "Medium Return").otherwise("Low Return"))

return_category_counts = ecommerce_return_df.groupBy("return_category").count()
return_category_counts.show()

avg_high_return_price = ecommerce_return_df.filter(col("return_category") == "High Return").agg(avg("sale_price").alias("avg_price"))
max_medium_return_rate = ecommerce_return_df.filter(col("return_category") == "Medium Return").agg(max("return_rate").alias("max_return_rate"))
avg_high_return_price.show()
max_medium_return_rate.show()

low_return_low_price_rate = ecommerce_return_df.filter((col("return_category") == "Low Return") & (col("sale_price") < 50) & (col("return_rate") < 5))
low_return_low_price_rate.show()
