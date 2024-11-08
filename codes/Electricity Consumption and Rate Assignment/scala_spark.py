import org.apache.spark.sql.functions._
val electricity_usage_df = List(
("House1", 550, 250),
("House2", 400, 180),
("House3", 150, 50),
("House4", 500, 200),
("House5", 600, 220),
("House6", 350, 120),
("House7", 100, 30),
("House8", 480, 190),
("House9", 220, 105),
("House10", 150, 60)
).toDF("household", "kwh_usage", "total_bill")

val electricity_usage_df = electricity_usage_df.withColumn( "usage_category", \
    when((col("kwh_usage") >= 200) & (col("kwh_usage") <= 500) & (col("total_bill") >= 100) & (col("total_bill") <= 200)), "Medium Usage").otherwise("Low Usage"))
val usage_category_counts = electricity_usage_df.groupBy("usage_category").count()
usage_category_counts.show()

val max_high_usage_bill = electricity_usage_df.filter(col("kwh_usage") > 500).agg(max("total_bill").alias("max_bill"))
max_high_usage_bill.show()

val avg_medium_usage_kwh = electricity_usage_df.filter(col("usage_category") == "Medium Usage").agg(avg("kwh_usage").alias("avg_kwh"))
avg_medium_usage_kwh.show()

val low_usage_high_kwh_count = electricity_usage_df.filter((col("usage_category") == "Low Usage") & (col("kwh_usage") > 300)).count()
print(low_usage_high_kwh_count)
