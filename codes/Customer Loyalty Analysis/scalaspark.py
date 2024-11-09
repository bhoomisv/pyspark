import org.apache.spark.sql.functions._
val customer_loyalty_df = List(
("Customer1", 25, 700),
("Customer2", 15, 400),
("Customer3", 5, 50),
("Customer4", 18, 450),
("Customer5", 22, 600),
("Customer6", 2, 80),
("Customer7", 12, 300),
("Customer8", 6, 150),
("Customer9", 10, 200),
("Customer10", 1, 90)
).toDF("customer_name", "purchase_frequency", "average_spending")

val customer_loyalty_df = customer_loyalty_df.withColumn("loyalty_level",when((col("purchase_frequency") > 20) & (col("average_spending") > 500), "Highly Loyal").when((col("purchase_frequency") >= 10) & (col("purchase_frequency") <= 20), "Moderately Loyal").otherwise("Low Loyalty"))

val loyalty_level_counts = customer_loyalty_df.groupBy("loyalty_level").count()
loyalty_level_counts.show()


val avg_highly_loyal_spending = customer_loyalty_df.filter(col("loyalty_level") == "Highly Loyal").agg(avg("average_spending").alias("avg_spending"))
avg_highly_loyal_spending.show()

val min_moderately_loyal_spending = customer_loyalty_df.filter(col("loyalty_level") == "Moderately Loyal").agg(min("average_spending").alias("min_spending"))
min_moderately_loyal_spending.show()

val low_loyalty_low_spending_freq = customer_loyalty_df.filter((col("loyalty_level") == "Low Loyalty") & (col("average_spending") < 100) & (col("purchase_frequency") < 5))
low_loyalty_low_spending_freq.show()
