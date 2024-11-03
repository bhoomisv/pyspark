
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder().appName("CustomerAgeGrouping").getOrCreate() 

customers = [
("karthik", 22),
("neha", 28),
("priya", 40),
  ("mohan", 55),
("ajay", 32),
("vijay", 18),
("veer", 47),
("aatish", 38),
("animesh", 60),
("nishad", 25)
]
customers_df = spark.createDataFrame(customers, ["name", "age"])
customers_df.createOrReplaceTempView("customers_table")

# Execute the SQL query
result_df = spark.sql("""
    SELECT age_group, COUNT(*) AS customer_count FROM (
        SELECT 
            INITCAP(name) AS name,
            age,
            CASE 
                WHEN age < 25 THEN 'Youth'
                WHEN age >= 25 AND age <= 45 THEN 'Adult'
                ELSE 'Senior'
            END AS age_group
        FROM customers_table
    )
    GROUP BY age_group
""")
result_df.show()


