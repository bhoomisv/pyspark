
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder().appName("CustomerAgeGrouping").getOrCreate() 

customers = [
("karthik", 22),
("neha", 28),
("priya", 40),
  ("mohan", 55),
("ajay", 32),
("vijay", 18),
("veer", 47),
("aatish", 38),
("animesh", 60),
("nishad", 25)
]
customers_df = spark.createDataFrame(customers, ["name", "age"])
customers_df = customers_df.withColumn("age_group", when(col("age") < 25, "Youth") \
                                                          .when((col("age") >= 25) & (col("age") <= 45),  "Adult") \
                                                          .otherwise("Senior")).withColumn("name", F.initcap(col("name")))

result_df = customers_df.groupBy("age_group").count().withColumnRenamed("count", "customer_count")
result_df.show()
