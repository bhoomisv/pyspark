'''8. Student Grade Classification
Classify students based on their scores as "Excellent" if score is 90 or above, "Good" if between 75-
89, and "Needs Improvement" if below 75. Count students in each category.'''
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("StudentGradeClassification").getOrCreate()   


val students = List(
("karthik", 95),
("neha", 82),
("priya", 74),
("mohan", 91),
("ajay", 67),
("vijay", 80),
("veer", 85),
("aatish", 72),
("animesh", 90),
("nishad", 60)
).toDF("name", "score")
val studentsWithGrade = studentsDF.withColumn("grade", when(col("score") >= 90, "Excellent") \
    .when((col("score") >= 75) & (col("score") < 90), "Good") \
    .otherwise("Needs Improvement"))

val gradeCounts = studentsWithGrade.groupBy("grade").count().withColumnRenamed("count", "student_count")
gradeCounts.show()


