import org.apache.spark.sql.functions._

val df = List(
  (1, "karthik", 1000),
  (2, "moahn", 2000),
  (3, "vinay", 1500),
  (4, "Deva", 3000)
).toDF("id", "name", "salary")

val dfWithRunningTotal = df.withColumn("running_total", sum("salary").over(Window.partitionBy("name").orderBy("id")))

dfWithRunningTotal.show()