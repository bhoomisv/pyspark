from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum, when, col, max, avg
spark = SparkSession.builder.appName("my code practice").getOrCreate()
electricity_usage = [
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
]
electricity_usage_df = spark.createDataFrame(electricity_usage, ["household", "kwh_usage", "total_bill"])
electricity_usage_df = electricity_usage_df.withColumn( "usage_category", \
    when((col("kwh_usage") >= 200) & (col("kwh_usage") <= 500) & (col("total_bill") >= 100) & (col("total_bill") <= 200)), "Medium Usage").otherwise("Low Usage"))
usage_category_counts = electricity_usage_df.groupBy("usage_category").count()
usage_category_counts.show()

max_high_usage_bill = electricity_usage_df.filter(col("kwh_usage") > 500).agg(max("total_bill").alias("max_bill"))
max_high_usage_bill.show()

avg_medium_usage_kwh = electricity_usage_df.filter(col("usage_category") == "Medium Usage").agg(avg("kwh_usage").alias("avg_kwh"))
avg_medium_usage_kwh.show()

low_usage_high_kwh_count = electricity_usage_df.filter((col("usage_category") == "Low Usage") & (col("kwh_usage") > 300)).count()
print(low_usage_high_kwh_count)
