
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum, when, col, max, avg, min, count, Window
spark = SparkSession.builder.appName("my code practice").getOrCreate()

students = [
("Student1", 70, 45, 60, 65, 75),
("Student2", 80, 55, 58, 62, 67),
("Student3", 65, 30, 45, 70, 55),
("Student4", 90, 85, 80, 78, 76),
("Student5", 72, 40, 50, 48, 52),
("Student6", 88, 60, 72, 70, 68),
("Student7", 74, 48, 62, 66, 70),
("Student8", 82, 56, 64, 60, 66),
("Student9", 78, 50, 48, 58, 55),
("Student10", 68, 35, 42, 52, 45)
]
students_df = spark.createDataFrame(students, ["student_id", "attendance_percentage","math_score", "science_score", "english_score", "history_score"])
students_df = students_df.withColumn("average_score", (col("math_score") + col("science_score") + col("english_score") + col("history_score")) / 4)
students_df.show()

students_df = students_df.withColumn("risk_level", \
    when((col("attendance_percentage") < 75) & (col("average_score") < 50), "At-Risk") \
    .when((col("attendance_percentage") >= 75) & (col("attendance_percentage") <= 85), "Moderate Risk") \
    .otherwise("Low Risk"))
students_df.show()

risk_level_counts = students_df.groupBy("risk_level").count()
risk_level_counts.show()

avg_at_risk_score = students_df.filter(col("risk_level") == "At-Risk").agg(avg("average_score").alias("avg_score"))
avg_at_risk_score.show()

moderate_risk_high_scorers = students_df.filter((col("risk_level") == "Moderate Risk") & (col("math_score") > 70) | (col("science_score") > 70) | (col("english_score") > 70) | (col("history_score") > 70)).groupby("student_id").agg(count("*").alias("high_score_count"))
moderate_risk_high_scorers.show()
moderate_risk_high_scorers= moderate_risk_high_scorers.filter(col("high_score_count") >= 3)
moderate_risk_high_scorers.show()










