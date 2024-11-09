

import org.apache.spark.sql.functions._

val df = List(
  (1, "karthik", 1000),
  (2, "moahn", 2000),
  (3, "vinay", 1500),
  (4, "Deva", 3000)
).toDF("id", "name", "salary")
val dfWithDiff = df.withColumn("min_salary", min("salary").over(Window.orderBy("id").rowsBetween(-2, 0))) \
  .withColumn("salary_diff", col("salary") - col("min_salary"))

dfWithDiff.show()