'''
Create a DataFrame that lists employees with names and their work status. For each employee,
determine if they are “Active” or “Inactive” based on the last check-in date. If the check-in date is
within the last 7 days, mark them as "Active"; otherwise, mark them as "Inactive." Ensure the first
letter of each name is capitalized.
'''

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff
spark = SparkSession.builder.appName("Employee Status").getOrCreate()
employees = [
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
]
employees_df = spark.createDataFrame(employees, ["name", "last_checkin"])
employees_df = employees_df.withColumn('days_since_last_checkin', datediff(F.current_date(), col('last_checkin')))
employees_df = employees_df.withColumn('status', when(col('days_since_last_checkin') <= 7, lit('Active')).otherwise(lit('Inactive')))
employees_df = employees_df.withColumn('name', F.initcap(col('name')))
final_df = employees_df.select('name', 'status')
final_df.show(10, False)
# spark.stop()

'''
+--------+-------+
|    name|  status|
+--------+-------+
|  Karthik| Inactive|
|     Neha|  Active|
|    Priya|  Active|
|    Mohan| Inactive|
|     Ajay| Inactive|
|    Vijay|  Active|
|     Veer|  Active|
|    Aatish| Inactive|
|  Animesh|  Active|
|    Nishad| Inactive|
|    Varun| Inactive|
|    Aadil| Inactive|
+--------+-------+
'''
