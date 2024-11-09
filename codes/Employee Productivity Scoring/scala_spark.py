import org.apache.spark.sql.functions._
val employee_productivity_df = List(
("Emp1", 85, 6),
("Emp2", 75, 4),
("Emp3", 40, 1),
("Emp4", 78, 5),
("Emp5", 90, 7),
("Emp6", 55, 3),
("Emp7", 80, 5),
("Emp8", 42, 2),
("Emp9", 30, 1),
("Emp10", 68, 4)
).toDF("employee_id", "productivity_score", "project_count")
employee_productivity_df.show()
val employee_productivity_df = employee_productivity_df.withColumn("performance_level",when((col("productivity_score") > 80) & (col("project_count") > 5), "High Performer").when((col("productivity_score") >= 60) & (col("productivity_score") <= 80), "Average Performer").otherwise("Low Performer"))
employee_productivity_df.show()
val performance_level_counts = employee_productivity_df.groupBy("performance_level").count()
performance_level_counts.show()

val avg_high_performer_score = employee_productivity_df.filter(col("performance_level") == "High Performer").agg(avg("productivity_score").alias("avg_score"))
val min_avg_performer_score = employee_productivity_df.filter(col("performance_level") == "Average Performer").agg(min("productivity_score").alias("min_score"))
avg_high_performer_score.show()
min_avg_performer_score.show()

val low_performer_low_score_count = employee_productivity_df.filter((col("performance_level") == "Low Performer") & (col("productivity_score") < 50) & (col("project_count") < 2))
low_performer_low_score_count.show()
