from pyspark.sql import SparkSession

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
employees_df.createOrReplaceTempView("employees_table")

result_df = spark.sql("""
    SELECT INITCAP(name) AS name, CASE WHEN DATEDIFF(CURRENT_DATE(), last_checkin) <= 7 THEN 'Active' ELSE 'Inactive' END AS status FROM employees_table
""")
result_df.show()
