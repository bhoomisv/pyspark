
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum, when, col, max, avg, min, count, Window
spark = SparkSession.builder.appName("my code practice").getOrCreate()

transactions = [
("Account1", "2024-11-01", 12000, 6, "Savings"),
("Account2", "2024-11-01", 8000, 3, "Current"),
("Account3", "2024-11-02", 2000, 1, "Savings"),
("Account4", "2024-11-02", 15000, 7, "Savings"),
("Account5", "2024-11-03", 9000, 4, "Current"),
("Account6", "2024-11-03", 3000, 1, "Current"),
("Account7", "2024-11-04", 13000, 5, "Savings"),
("Account8", "2024-11-04", 6000, 2, "Current"),
("Account9", "2024-11-05", 20000, 8, "Savings"),
("Account10", "2024-11-05", 7000, 3, "Savings")
]
transactions_df = spark.createDataFrame(transactions, ["account_id", "transaction_date", "amount", "frequency", "account_type"])

transactions_df = transactions_df.withColumn("risk_level", \ 
    when((col("amount") > 10000) & (col("frequency") > 5), "High Risk") \
    .when((col("amount") >= 5000) & (col("amount") <= 10000) & (col("frequency") >= 2) & (col("frequency") <= 5), "Moderate Risk") \
    .otherwise("Low Risk"))
transactions_df.show()

risk_level_counts = transactions_df.groupBy("risk_level").count()
risk_level_counts.show()

windowSpec = Window.partitionBy("account_id")
high_risk_accounts = transactions_df.filter(col("risk_level") == "High Risk").withColumn("total_amount", F.sum("amount").over(windowSpec))
high_risk_accounts.show()

moderate_risk_savings_high_amount = transactions_df.filter((col("risk_level") == "Moderate Risk") & (col("account_type") == "Savings") & (col("amount") > 7500))
moderate_risk_savings_high_amount.show()

