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
vehicles_df.createOrReplaceTempView("vehicles_table")

result_df = spark.sql("""
        SELECT 
            vehicle_name,
            mileage,
            CASE 
                WHEN mileage > 25 THEN 'High Efficiency'
                WHEN mileage >= 15 AND mileage <= 25 THEN 'Moderate Efficiency'
                ELSE 'Low Efficiency'
            END AS mileage_category
        FROM vehicles_table
""")
result_df.show()




