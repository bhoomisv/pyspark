
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("my code practice").getOrCreate()   
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


val productsWithClassification = products.withColumn("return_rate_classification",  \
  when(col("return_count") > 100 && col("satisfaction_score") < 50, lit("High Return Rate")) \
  .when((col("return_count") >= 50 && col("return_count") <= 100) && (col("satisfaction_score") >= 50 && col("satisfaction_score") <= 70),lit("Moderate Return Rate")) \
  .otherwise(lit("Low Return Rate"))
)
val returnRateCounts = productsWithClassification.groupBy("category", "return_rate_classification").count()

returnRateCounts.show()
