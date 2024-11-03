'''10. Employee Bonus Calculation Based on Performance and Department
Classify employees for a bonus eligibility program. Employees in "Sales" and "Marketing" with
performance scores above 80 get a 20% bonus, while others with scores above 70 get 15%. All other
employees receive no bonus. Group by department and calculate total bonus allocation.'''

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("BonusCalculation").getOrCreate()   


val employees = List(
("karthik", "Sales", 85),
("neha", "Marketing", 78),
("priya", "IT", 90),
("mohan", "Finance", 65),
("ajay", "Sales", 55),
("vijay", "Marketing", 82),
("veer", "HR", 72),
("aatish", "Sales", 88),
("animesh", "Finance", 95),
("nishad", "IT", 60)
).toDF("name", "department", "performance_score")
val employeesWithBonus = employeesDF.withColumn("bonus_percentage", 
    when((col("department") == "Sales") | (col("department") == "Marketing"), \
      when(col("performance_score") > 80, 0.2).otherwise(0) \
    ).otherwise(when(col("performance_score") > 70, 0.15).otherwise(0))) \
  .withColumn("bonus_amount", col("performance_score") * col("bonus_percentage"))

val bonusByDepartment = employeesWithBonus.groupBy("department").agg(sum("bonus_amount").as("total_bonus"))

bonusByDepartment.show()
