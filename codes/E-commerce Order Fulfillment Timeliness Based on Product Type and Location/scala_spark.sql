

import org.apache.spark.sql.functions._

val orders = List(
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
).toDF("order_id", "product_type", "origin", "delivery_days")
val orders_df = orders_df.withColumn("delivery_speed", \
    when(col("delivery_days") <= 3, "Fast").otherwise("Slow"))

val order_counts = orders_df.groupBy("product_type", "delivery_speed").count()

print(order_counts)
