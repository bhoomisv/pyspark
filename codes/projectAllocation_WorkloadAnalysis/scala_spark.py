'''Question 3: Project Allocation and Workload Analysis
Given a DataFrame with project allocation data for multiple employees, determine each employee's
workload level based on their hours worked in a month across various projects. Categorize
employees as “Overloaded” if they work more than 200 hours, “Balanced” if between 100-200 hours,
and “Underutilized” if below 100 hours. Capitalize each employee’s name, and show the aggregated
workload status count by category.
'''

import org.apache.spark.sql.functions._
val workload = List(
("karthik", "ProjectA", 120),
("karthik", "ProjectB", 100),
("neha", "ProjectC", 80),
("neha", "ProjectD", 30),
("priya", "ProjectE", 110),
("mohan", "ProjectF", 40),
("ajay", "ProjectG", 70),
("vijay", "ProjectH", 150),
("veer", "ProjectI", 190),
("aatish", "ProjectJ", 60),
("animesh", "ProjectK", 95),
("nishad", "ProjectL", 210),
("varun", "ProjectM", 50),
("aadil", "ProjectN", 90)
).toDF("name", "project", "hours")

val workloadByEmployee = workloadDF.groupBy("name") \
                                    .agg(sum("hours").as("total_hours")) \
                                    .withColumn("name", initcap(col("name"))) \
                                    .withColumn("workload_status",  when(col("total_hours") > 200, "Overloaded") \
                                      .when((col("total_hours") >= 100) & (col("total_hours") <= 200), "Balanced") \
                                      .otherwise("Underutilized")
  )

val workloadStatusCounts = workloadByEmployee.groupBy("workload_status").count().withColumnRenamed("count", "employee_count")

workloadStatusCounts.show()
