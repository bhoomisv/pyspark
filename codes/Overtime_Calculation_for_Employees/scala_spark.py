'''5. Overtime Calculation for Employees
Determine whether an employee has "Excessive Overtime" if their weekly hours exceed 60,
"Standard Overtime" if between 45-60 hours, and "No Overtime" if below 45 hours. Capitalize each
name and group by overtime status.
'''

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("OvertimeAnalysis").getOrCreate()   

val employees = List(
("karthik", 62),
("neha", 50),
("priya", 30),
("mohan", 65),
("ajay", 40),
("vijay", 47),
("veer", 55),
("aatish", 30),
("animesh", 75),
("nishad", 60)
).toDF("name", "hours_worked")
val employeesWithOvertimeStatus = employeesDF.withColumn("name", initcap(col("name"))) \
  .withColumn("overtime_status", when(col("hours_worked") > 60, "Excessive Overtime") \
    .when((col("hours_worked") >= 45) & (col("hours_worked") <= 60), "Standard Overtime") \
    .otherwise("No Overtime")
  )
val overtimeStatusCounts = employeesWithOvertimeStatus.groupBy("overtime_status").count().withColumnRenamed("count", "employee_count")
overtimeStatusCounts.show()


