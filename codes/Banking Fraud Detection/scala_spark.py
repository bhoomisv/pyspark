import org.apache.spark.sql.functions._
val transactions = List(
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
).toDF("account_id", "transaction_date", "amount", "frequency", "account_type")

val transactionsWithRisk = transactions.withColumn("risk_level", \
  when((col("amount") > 10000) && (col("frequency") > 5), lit("High Risk")) \
    .when((col("amount") >= 5000) && (col("amount") <= 10000) && (col("frequency") >= 2) && (col("frequency") <= 5), lit("Moderate Risk")) \
    .otherwise(lit("Low Risk")))
transactionsWithRisk.show()
val riskLevelCounts = transactionsWithRisk.groupBy("risk_level").count()
riskLevelCounts.show()

val windowSpec = Window.partitionBy("account_id")
val highRiskAccounts = transactionsWithRisk.filter(col("risk_level") === "High Risk").withColumn("total_amount", sum("amount").over(windowSpec))
highRiskAccounts.show()

val moderateRiskSavingsHighAmount = transactionsWithRisk.filter((col("risk_level") === "Moderate Risk") && (col("account_type") === "Savings") && (col("amount") > 7500))
moderateRiskSavingsHighAmount.show()

