
customer_loyalty_df = customer_loyalty_df.withColumn("loyalty_level",when((col("purchase_frequency") > 20) & (col("average_spending") > 500), "Highly Loyal").when((col("purchase_frequency") >= 10) & (col("purchase_frequency") <= 20), "Moderately Loyal").otherwise("Low Loyalty"))


loyalty_level_counts = customer_loyalty_df.groupBy("loyalty_level").count()
loyalty_level_counts.show()


avg_highly_loyal_spending = customer_loyalty_df.filter(col("loyalty_level") == "Highly Loyal").agg(avg("average_spending").alias("avg_spending"))
avg_highly_loyal_spending.show()


min_moderately_loyal_spending = customer_loyalty_df.filter(col("loyalty_level") == "Moderately Loyal").agg(min("average_spending").alias("min_spending"))
min_moderately_loyal_spending.show()

low_loyalty_low_spending_freq = customer_loyalty_df.filter((col("loyalty_level") == "Low Loyalty") & (col("average_spending") < 100) & (col("purchase_frequency") < 5))
low_loyalty_low_spending_freq.show()
