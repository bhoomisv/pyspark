

import org.apache.spark.sql.functions._

val df = List(
  (1, "KitKat", 1000.0, "2021-01-01"),
  (1, "KitKat", 2000.0, "2021-01-02"),
  (1, "KitKat", 1000.0, "2021-01-03"),
  (1, "KitKat", 2000.0, "2021-01-04"),
  (1, "KitKat", 3000.0, "2021-01-05"),
  (1, "KitKat", 1000.0, "2021-01-06")
).toDF("IT_ID", "IT_Name", "Price", "PriceDate")

val dfWithDiff = df.withColumn("PrevPrice", lag("Price", 1).over(Window.partitionBy("IT_ID").orderBy("PriceDate"))) \
  .withColumn("PrevDate", lag("PriceDate", 1).over(Window.partitionBy("IT_ID").orderBy("PriceDate"))) \
  .withColumn("PriceDiff", col("Price") - col("PrevPrice")) \
  .withColumn("DaysDiff", datediff(col("PriceDate"), col("PrevDate"))) \
  .withColumn("PriceDiffPerDay", col("PriceDiff") / col("DaysDiff")) \
  .select("IT_ID", "IT_Name", "PriceDate", "Price", "PrevPrice", "PriceDiff", "DaysDiff", "PriceDiffPerDay")

dfWithDiff.show()
