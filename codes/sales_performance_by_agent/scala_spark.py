'''Given a DataFrame of sales agents with their total sales amounts, calculate the performance status
based on sales thresholds: “Excellent” if sales are above 50,000, “Good” if between 25,000 and
50,000, and “Needs Improvement” if below 25,000. Capitalize each agent's name, and show total
sales aggregated by performance status.
'''
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("SalesPerformance").getOrCreate()   

val sales = List(
("karthik", 60000),
("neha", 48000),
("priya", 30000),
("mohan", 24000),
("ajay", 52000),
("vijay", 45000),
("veer", 70000),
("aatish", 23000),
("animesh", 15000),
("nishad", 8000),
("varun", 29000),
("aadil", 32000)
).toDF("name", "total_sales")


val salesWithStatus = salesDF.withColumn("name", initcap("name")) \
  .withColumn("performance_status",  when($"total_sales" > 50000, "Excellent") \
    .when($"total_sales" >= 25000 && $"total_sales" <= 50000, "Good") \
    .otherwise("Needs Improvement"))

val salesByStatus = salesWithStatus.groupBy("performance_status").agg(sum("total_sales").as("total_sales"))

salesByStatus.show()
