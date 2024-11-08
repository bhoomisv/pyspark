from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum, when, col, max, avg
spark = SparkSession.builder.appName("my code practice").getOrCreate()

customers = [
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
]
customers_df = spark.createDataFrame(customers, ["name", "membership", "spending", "age"])
customers_df = customers_df.withColumn("spending_category", \
    when(col("spending") > 1000, "High Spender").otherwise("Low Spender"))

avg_spending_by_membership = customers_df.groupBy("membership") \
.agg(avg("spending").alias("avg_spending"))

avg_spending_by_membership.show()
