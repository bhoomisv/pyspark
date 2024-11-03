from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder.appName("Student Grade Classification").getOrCreate()

students = [
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
]
students_df = spark.createDataFrame(students, ["name", "score"])
students_df = students_df.withColumn("grade", when(col("score") >= 90, "Excellent") /
                                                      .when((col("score") >= 75) & (col("score") <= 89), "Good") /
                                                      .otherwise("Needs Improvement")) /
                                          .withColumn("name", F.initcap(col("name")))

result_df = students_df.groupBy("grade").count().withColumnRenamed("count", "student_count")
result_df.show()
