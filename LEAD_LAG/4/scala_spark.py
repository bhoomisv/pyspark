
import org.apache.spark.sql.functions._

val df = List(
  (1, "karthik", 1000),
  (2, "moahn", 2000),
  (3, "vinay", 1500),
  (4, "Deva", 3000)
).toDF("id", "name", "salary")

val dfWithChange = df.withColumn("PrevSalary", lag("salary", 1).over(Window.orderBy("id")))
  .withColumn("SalaryChangePercent", (col("salary") - col("PrevSalary")) / col("PrevSalary") * 100)
  .select("id", "name", "salary", "PrevSalary", "SalaryChangePercent")

dfWithChange.show()