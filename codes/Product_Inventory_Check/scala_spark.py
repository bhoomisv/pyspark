'''9. Product Inventory Check
Classify inventory stock levels as "Overstocked" if stock exceeds 100, "Normal" if between 50-100,
and "Low Stock" if below 50. Aggregate total stock in each category.'''
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("InventoryCheck").getOrCreate()   


val inventory = List(
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
).toDF("product_name", "stock_quantity")
val inventoryWithStatus = inventoryDF.withColumn("stock_status", when(col("stock_quantity") > 100, "Overstocked") \
    .when((col("stock_quantity") >= 50) & (col("stock_quantity") <= 100), "Normal") \
    .otherwise("Low Stock"))

val stockByStatus = inventoryWithStatus.groupBy("stock_status").agg(sum("stock_quantity").as("total_stock"))

stockByStatus.show()
