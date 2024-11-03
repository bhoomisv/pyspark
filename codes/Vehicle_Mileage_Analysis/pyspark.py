
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder.appName("Vehicle Mileage Analysis").getOrCreate()

vehicles = [
("CarA", 30),
  ("CarB", 22),
("CarC", 18),
("CarD", 15),
("CarE", 10),
("CarF", 28),
("CarG", 12),
("CarH", 35),
("CarI", 25),
("CarJ", 16)
]
vehicles_df = spark.createDataFrame(vehicles, ["vehicle_name", "mileage"])
result_df = vehicles_df.withColumn("mileage_category", when(col("mileage") > 25, "High Efficiency") \
                                                                 .when((col("mileage") >= 15) & (col("mileage") <= 25), "Moderate Efficiency") \
                                                                 .otherwise("Low Efficiency"))

result_df.show()
