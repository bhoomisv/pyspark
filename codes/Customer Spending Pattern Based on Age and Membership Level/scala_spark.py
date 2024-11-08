
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("my code practice").getOrCreate()   

val customers = List(
("karthik", "Premium", 1050, 32),
("neha", "Standard", 800, 28),
("priya", "Premium", 1200, 40),
("mohan", "Basic", 300, 35),
("ajay", "Standard", 700, 25),
("vijay", "Premium", 500, 45),
("veer", "Basic", 450, 33),
("aatish", "Standard", 600, 29),
("animesh", "Premium", 1500, 60),
("nishad", "Basic", 200, 21)
).toDF("name", "membership", "spending", "age")

val customersWithCategory = customers.withColumn("spending_category", \
  when(col("spending") > 1000, lit("High Spender")).otherwise(lit("Low Spender")))

val avgSpendingByMembership = customersWithCategory.groupBy("membership").agg(avg("spending").alias("avg_spending"))

avgSpendingByMembership.show()
