
import org.apache.spark.sql.functions._

val customer_purchases_df = List(
("karthik", "Premium", 50, 5000),
("neha", "Standard", 10, 2000),
("priya", "Premium", 65, 8000),
("mohan", "Basic", 90, 1200),
("ajay", "Standard", 25, 3500),
("vijay", "Premium", 15, 7000),
("veer", "Basic", 75, 1500),
("aatish", "Standard", 45, 3000),
("animesh", "Premium", 20, 9000),
("nishad", "Basic", 80, 1100)
).toDF("name", "membership", "days_since_last_purchase", "total_purchase_amount")

val customer_purchases_df = customer_purchases_df.withColumn( "purchase_recency", \
    when(col("days_since_last_purchase") <= 30, "Frequent").when(col("days_since_last_purchase") <= 60, "Occasional").otherwise("Rare"))

val recency_counts = customer_purchases_df.groupBy("membership", "purchase_recency").count()
recency_counts.show()

val avg_frequent_premium_purchase = customer_purchases_df.filter((col("purchase_recency") == "Frequent") & (col("membership") == "Premium")) \
  .agg(avg("total_purchase_amount").alias("avg_purchase_amount"))
avg_frequent_premium_purchase.show()

val min_rare_purchase_by_membership = customer_purchases_df.filter(col("purchase_recency") == "Rare").groupBy("membership") \
  .agg(min("total_purchase_amount").alias("min_purchase_amount"))
min_rare_purchase_by_membership.show()
