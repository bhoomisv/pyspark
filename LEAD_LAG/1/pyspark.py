#we want to find the difference between the price on each day with it’s previous day.

from pyspark.sql.functions import col, lag, datediff
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("my code practice").getOrCreate()
data = [
    (1, "KitKat", 1000.0, "2021-01-01"),
    (1, "KitKat", 2000.0, "2021-01-02"),
    (1, "KitKat", 1000.0, "2021-01-03"),
    (1, "KitKat", 2000.0, "2021-01-04"),
    (1, "KitKat", 3000.0, "2021-01-05"),
    (1, "KitKat", 1000.0, "2021-01-06")
]
df = spark.createDataFrame(data, ["IT_ID", "IT_Name", "Price", "PriceDate"])

df1 = df.withColumn("PrevPrice", lag("Price").over(Window.partitionBy("IT_ID").orderBy("PriceDate"))) \
    .withColumn("PrevDate", lag("PriceDate").over(Window.partitionBy("IT_ID").orderBy("PriceDate"))) \
    .withColumn("PriceDiff", col("Price") - col("PrevPrice")) \
    .withColumn("DaysDiff", datediff(col("PriceDate"), col("PrevDate"))) \
    .withColumn("PriceDiffPerDay", col("PriceDiff") / col("DaysDiff")) \
    .select("IT_ID", "IT_Name", "PriceDate", "Price", "PrevPrice", "PriceDiff", "DaysDiff", "PriceDiffPerDay")
df1.show()
