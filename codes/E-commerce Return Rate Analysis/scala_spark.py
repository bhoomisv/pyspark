import org.apache.spark.sql.functions._

val ecommerce_return_df = List(
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
).toDF("product_name", "sale_price", "return_rate")


val ecommerce_return_df = ecommerce_return_df.withColumn("return_category", when(col("return_rate") > 20, "High Return").when((col("return_rate") >= 10) & (col("return_rate") <= 20), "Medium Return").otherwise("Low Return"))
ecommerce_return_df.show()
val return_category_counts = ecommerce_return_df.groupBy("return_category").count()
return_category_counts.show()

val avg_high_return_price = ecommerce_return_df.filter(col("return_category") == "High Return").agg(avg("sale_price").alias("avg_price"))
val max_medium_return_rate = ecommerce_return_df.filter(col("return_category") == "Medium Return").agg(max("return_rate").alias("max_return_rate"))
avg_high_return_price.show()
max_medium_return_rate.show()

val low_return_low_price_rate = ecommerce_return_df.filter((col("return_category") == "Low Return") & (col("sale_price") < 50) & (col("return_rate") < 5))
low_return_low_price_rate.show()
