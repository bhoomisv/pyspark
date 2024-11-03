from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Sales Performance").getOrCreate()
sales = [
    ("karthik", 60000),
    ("neha", 48000),
    ("priya", 30000),
    ("mohan", 24000),
    ("ajay", 52000),
    ("vijay", 45000),
    ("veer", 70000),
    ("aatish", 23000),
    ("animesh", 15000),
    ("nishad", 8000),
    ("varun", 29000),
    ("aadil", 32000)
]
sales_df = spark.createDataFrame(sales, ["name", "total_sales"])
sales_df.createOrReplaceTempView("sales_table")

result_df = spark.sql("""
    SELECT performance_status, SUM(total_sales) AS total_sales FROM (
        SELECT INITCAP(name) AS name, total_sales,
            CASE 
                WHEN total_sales > 50000 THEN 'Excellent'
                WHEN total_sales >= 25000 AND total_sales >= 50000 THEN 'Good'
                ELSE 'Needs Improvement'
            END AS performance_status
        FROM sales_table
    )
    GROUP BY performance_status
""")

result_df.show()
