import org.apache.spark.sql.functions._

val df = List(
  (1, "karthik", 1000),
  (2, "moahn", 2000),
  (3, "vinay", 1500),
  (4, "Deva", 3000)
).toDF("id", "name", "salary")

val dfWithDiff = df.withColumn("PrevSalary", lag("salary", 1).over(Window.partitionBy("name").orderBy("id"))) \
  .withColumn("SalaryDiff", abs(col("salary") - col("PrevSalary")))
dfWithDiff.show()
val filteredDf = dfWithDiff.filter(col("SalaryDiff") > 500)
filteredDf.show()
val dfWithLeadLag = filteredDf.withColumn("lead_salary", lead("salary", 1).over(Window.partitionBy("name").orderBy("id"))) \
  .withColumn("lag_salary", lag("salary", 1).over(Window.partitionBy("name").orderBy("id")))

dfWithLeadLag.show()