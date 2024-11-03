import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("ProductReturnAnalysis").getOrCreate()
val products = List(
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
).toDF("product_name", "category", "return_count", "satisfaction_score")

val productsWithReturnRate = productsDF.withColumn("return_rate_class", \
    when((col("return_count") > 100) & (col("satisfaction_score") < 50), "High Return Rate") \
    .when((col("return_count") >= 50) & (col("return_count") <= 100) & (col("satisfaction_score") >= 50) & (col("satisfaction_score") <= 70), "Moderate Return Rate") \
    .otherwise("Low Return Rate"))

val returnRateByCategory = productsWithReturnRate.groupBy("category", "return_rate_class").count().withColumnRenamed("count", "product_count")

returnRateByCategory.show()
