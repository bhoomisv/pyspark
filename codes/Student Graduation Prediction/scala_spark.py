
import org.apache.spark.sql.functions._
val students = List(
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
).toDF("student_id", "attendance_percentage", "math_score", "science_score", "english_score","history_score")

val studentsWithAvgScore = students.withColumn("average_score", (col("math_score") + col("science_score") + col("english_score") + col("history_score")) / 4)
studentsWithAvgScore.show()
val studentsWithRisk = studentsWithAvgScore.withColumn("risk_level", \
  when((col("attendance_percentage") < 75) && (col("average_score") < 50), lit("At-Risk")) \
    .when((col("attendance_percentage") >= 75) && (col("attendance_percentage") <= 85), lit("Moderate Risk")) \
    .otherwise(lit("Low Risk")))

val riskLevelCounts = studentsWithRisk.groupBy("risk_level").count()
riskLevelCounts.show()

val avgAtRiskScore = studentsWithRisk.filter(col("risk_level") === "At-Risk").agg(avg("average_score").alias("avg_score"))
avgAtRiskScore.show()

val moderateRiskHighScorers = studentsWithRisk.filter(col("risk_level") === "Moderate Risk") \
      .withColumn("high_score_count", when(col("math_score") > 70, 1).otherwise(0) + \
                                      when(col("science_score") > 70, 1).otherwise(0) + \
                                      when(col("english_score") > 70, 1).otherwise(0) + \
                                      when(col("history_score") > 70, 1).otherwise(0)) \
      .filter(col("high_score_count") >= 3)
moderateRiskHighScorers.show()
