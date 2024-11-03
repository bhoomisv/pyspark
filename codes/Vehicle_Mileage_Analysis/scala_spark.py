'''
7. Vehicle Mileage Analysis
Classify each vehicle’s mileage as "High Efficiency" if mileage is above 25 MPG, "Moderate Efficiency"
if between 15-25 MPG, and "Low Efficiency" if below 15 MPG. '''
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("VehicleMileageAnalysis").getOrCreate()   

val vehicles = List(
("CarA", 30),
("CarB", 22),
("CarC", 18),
("CarD", 15),
("CarE", 10),
("CarF", 28),
("CarG", 12),
("CarH", 35),
("CarI", 25),
("CarJ", 16)
).toDF("vehicle_name", "mileage")
val vehiclesWithMileageClass = vehiclesDF.withColumn("mileage_class", when(col("mileage") > 25, "High Efficiency") \
    .when((col("mileage") >= 15) & (col("mileage") <= 25), "Moderate Efficiency") \
    .otherwise("Low Efficiency")
  )

vehiclesWithMileageClass.show()
