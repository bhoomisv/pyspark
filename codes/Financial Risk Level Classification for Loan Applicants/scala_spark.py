import org.apache.spark.sql.functions._

val loan_applicants_df = List(
("karthik", 60000, 120000, 590),
("neha", 90000, 180000, 610),
("priya", 50000, 75000, 680),
("mohan", 120000, 240000, 560),
("ajay", 45000, 60000, 620),
("vijay", 100000, 100000, 700),
("veer", 30000, 90000, 580),
("aatish", 85000, 85000, 710),
("animesh", 50000, 100000, 650),
("nishad", 75000, 200000, 540)
).toDF("name", "income", "loan_amount", "credit_score")

val loan_applicants_df = loan_applicants_df.withColumn("risk_level", \
    when((col("loan_amount") > 2 * col("income")) & (col("credit_score") < 600), "High Risk"). \
    when( (col("loan_amount") >= col("income")) & (col("loan_amount") <= 2 * col("income")) & (col("credit_score") >= 600) & (col("credit_score") <= 700),"Moderate Risk"). \
    otherwise("Low Risk"))

val risk_level_counts = loan_applicants_df.groupBy("risk_level").count()
risk_level_counts.show()

val high_risk_avg_loan_amount = loan_applicants_df.filter(col("risk_level") == "High Risk") \
                    .groupBy(when(col("income") < 50000, "<50k").when(col("income") >= 50000, "<100k").otherwise(">100k").alias("income_range"))
                    .agg(avg("loan_amount").alias("avg_loan_amount"))
high_risk_avg_loan_amount.show()

val avg_credit_score_by_income_range = loan_applicants_df.groupBy(when(col("income") < 50000, "<50k").when(col("income") >= 50000, "<100k").otherwise(">100k").alias("income_range"),"risk_level"). \
      agg(avg("credit_score").alias("avg_credit_score"))
val avg_credit_score_by_income_range_filter = avg_credit_score_by_income_range.filter(col("avg_credit_score") < 650)
avg_credit_score_by_income_range_filter.show()
