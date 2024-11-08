from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder.appName("my code practice").getOrCreate()
orders = [
("Order1", "Laptop", "Domestic", 2),
("Order2", "Shoes", "International", 8),
("Order3", "Smartphone", "Domestic", 3),
("Order4", "Tablet", "International", 5),
("Order5", "Watch", "Domestic", 7),
("Order6", "Headphones", "International", 10),
("Order7", "Camera", "Domestic", 1),
("Order8", "Shoes", "International", 9),
("Order9", "Laptop", "Domestic", 6),
("Order10", "Tablet", "International", 4)
]
orders_df = spark.createDataFrame(orders, ["order_id", "product_type", "origin", "delivery_days"])
orders_df = orders_df.withColumn("delivery_speed", \
    when(col("delivery_days") <= 3, "Fast").otherwise("Slow"))

order_counts = orders_df.groupBy("product_type", "delivery_speed").count()

order_counts.show()
