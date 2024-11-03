'''6. Customer Age Grouping
Group customers as "Youth" if under 25, "Adult" if between 25-45, and "Senior" if over 45. Capitalize
names and show total customers in each group.'''
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("CustomerAgeGrouping").getOrCreate()   

val customers = List(
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
).toDF("name", "age")
val customersWithAgeGroup = customersDF.withColumn("name", initcap(col("name"))).withColumn("age_group", when(col("age") < 25, "Youth") \
    .when((col("age") >= 25) & (col("age") <= 45), "Adult") \
    .otherwise("Senior"))
val ageGroupCounts = customersWithAgeGroup.groupBy("age_group").count().withColumnRenamed("count", "customer_count")
ageGroupCounts.show()


