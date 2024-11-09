
import org.apache.spark.sql.functions._

val df = List(
  (1, "John", 1000, "2016-01-01"),
  (1, "John", 2000, "2016-02-01"),
  (1, "John", 1000, "2016-03-01"),
  (1, "John", 2000, "2016-04-01"),
  (1, "John", 3000, "2016-05-01"),
  (1, "John", 1000, "2016-06-01")
).toDF("ID", "NAME", "SALARY", "DATE")

val dfWithDiff = df.withColumn("PrevSalary", lag("SALARY", 1).over(Window.partitionBy("ID").orderBy("DATE"))) \
  .withColumn("SalaryChange", when(col("SALARY") > col("PrevSalary"), "UP") \
                                   .when(col("SALARY") < col("PrevSalary"), "DOWN") \
                                   .otherwise("SAME")) \
  .select("ID", "NAME", "DATE", "SALARY", "PrevSalary", "SalaryChange")

dfWithDiff.show()