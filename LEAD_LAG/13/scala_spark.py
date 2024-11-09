
import org.apache.spark.sql.functions._

val df = List(
  (1, "karthik", 1000),
  (2, "moahn", 2000),
  (3, "vinay", 1500),
  (4, "Deva", 3000)
).toDF("id", "name", "salary")

val dfWithAvgDiff = df.withColumn("avg_salary", avg("salary").over(Window.partitionBy("name"))) \
  .withColumn("salary_diff", col("salary") - col("avg_salary"))

dfWithAvgDiff.show()