import org.apache.spark.sql.functions._

val df = List(
  (1, "karthik", 1000),
  (2, "moahn", 2000),
  (3, "vinay", 1500),
  (4, "Deva", 3000)
).toDF("id", "name", "salary")

val filteredDf = df.filter(col("salary") > 1500)
filteredDf.show()
val dfWithLeadLag = filteredDf.withColumn("lead_salary", lead("salary", 1).over(Window.orderBy("id"))) \
  .withColumn("lag_salary", lag("salary", 1).over(Window.orderBy("id")))

dfWithLeadLag.show()