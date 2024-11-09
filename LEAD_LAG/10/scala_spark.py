import org.apache.spark.sql.functions._

val df = List(
  (1, "karthik", 1000),
  (2, "moahn", 2000),
  (3, "vinay", 1500),
  (4, "Deva", 3000)
).toDF("id", "name", "salary")

val dfWithCumulativeCount = df.withColumn("cumulative_count", sum(lit(1)).over(Window.partitionBy("name").orderBy("id")))

dfWithCumulativeCount.show()