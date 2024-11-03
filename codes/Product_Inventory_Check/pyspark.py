
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum


spark = SparkSession.builder.appName("Inventory Analysis").getOrCreate()

inventory = [
("ProductA", 120),
("ProductB", 95),
("ProductC", 45),
("ProductD", 200),
("ProductE", 75),
("ProductF", 30),
("ProductG", 85),
("ProductH", 100),
("ProductI", 60),
("ProductJ", 20)
]
inventory_df = spark.createDataFrame(inventory, ["product_name", "stock_quantity"])
inventory_df = inventory_df.withColumn("stock_status", when(col("stock_quantity") > 100, "Overstocked") \
                                                                 .when((col("stock_quantity") >= 50) & (col("stock_quantity") <= 100), "Normal") \
                                                                 .otherwise("Low Stock"))

result_df = inventory_df.groupBy("stock_status").agg(F.sum("stock_quantity").alias("total_stock"))
result_df.show()
