import org.apache.spark.sql.functions._
val employees_df = List(
("karthik", "IT", 110000, 12, 88),
("neha", "Finance", 75000, 8, 70),
("priya", "IT", 50000, 5, 65),
("mohan", "HR", 120000, 15, 92),
("ajay", "IT", 45000, 3, 50),
("vijay", "Finance", 80000, 7, 78),
("veer", "Marketing", 95000, 6, 85),
("aatish", "HR", 100000, 9, 82),
("animesh", "Finance", 105000, 11, 88),
("nishad", "IT", 30000, 2, 55)
).toDF("name", "department", "salary", "experience", "performance_score")


val employees_df = employees_df.withColumn("salary_band",when((col("salary") > 100000) & (col("experience") > 10), "Senior").when((col("salary") >= 50000) & (col("salary") <= 100000) & (col("experience") >= 5) & (col("experience") <= 10), "Mid-level").otherwise("Junior"))
val employee_counts = employees_df.groupBy("department", "salary_band").count()
employee_counts.show()

val avg_performance_by_band = employees_df.groupBy("salary_band").agg(avg("performance_score").alias("avg_performance"))
avg_performance_by_band.filter(col("avg_performance") > 80).show()

val mid_level_high_performers = employees_df.filter((col("salary_band") == "Mid-level") & (col("performance_score") > 85) & (col("experience") > 7))
mid_level_high_performers.show()
