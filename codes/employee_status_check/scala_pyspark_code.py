'''Create a DataFrame that lists employees with names and their work status. For each employee,
determine if they are “Active” or “Inactive” based on the last check-in date. If the check-in date is
within the last 7 days, mark them as "Active"; otherwise, mark them as "Inactive." Ensure the first
letter of each name is capitalized.'''

import org.apache.spark.sql.functions._
val employees = List(
("karthik", "2024-11-01"),
("neha", "2024-10-20"),
("priya", "2024-10-28"),
("mohan", "2024-11-02"),
("ajay", "2024-09-15"),
("vijay", "2024-10-30"),
("veer", "2024-10-25"),
("aatish", "2024-10-10"),
("animesh", "2024-10-15"),
("nishad", "2024-11-01"),
("varun", "2024-10-05"),
("aadil", "2024-09-30")
).toDF("name", "last_checkin")

val dfWithDate = employeesDF.withColumn("last_checkin_date", to_date(col("last_checkin"), "yyyy-MM-dd"))

val dfWithDiff = dfWithDate.withColumn("days_since_last_checkin", datediff(current_date(), col("last_checkin_date")))

val dfWithStatus = dfWithDiff.withColumn("status", when(col("days_since_last_checkin") <= 7, "Active").otherwise("Inactive"))

val dfWithCapitalizedNames = dfWithStatus.withColumn(col("name"), initcap("name"))

val finalDF = dfWithCapitalizedNames.select("name", "status")

finalDF.show()

'''
+--------+-------+
|   name  |status|
+--------+-------+
|Karthik | Active|
|    Neha|Inactive|
|   Priya|Inactive|
|   Mohan| Active|
|    Ajay|Inactive|
|   Vijay|Inactive|
|    Veer|Inactive|
|  Aatish|Inactive|
|Animesh|Inactive|
|  Nishad| Active|
|   Varun|Inactive|
|   Aadil|Inactive|
+--------+-------+
'''

