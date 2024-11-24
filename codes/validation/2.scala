import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object my_object {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("my_object")
      .master("local[*]")
      .getOrCreate()

    
    val data = Seq(
      ("Karthik", "IT", "Male"),
      ("Pratik", "IT", "Male"),
      ("Hari", "HR", "Male"),
      ("Mohan", "IT", "Male"),
      ("Veer", "HR", "Male"),
      ("Veena", "IT", "Female"),
      ("Mohana", "HR", "Female")
    )

    val df = spark.createDataFrame(data).toDF("Name", "Department", "Gender")

    
    val header = df.first()
    val trailer = df.collect().last
    val expectedRecordCount = 7

    if (header.getString(0) == "EMPLOYEE" && header.getString(1) == "v1" && header.getString(2).toInt == expectedRecordCount &&
      trailer.getString(0) == "TRAILER" && trailer.getString(1).toInt == expectedRecordCount &&
      df.count() - 2 == expectedRecordCount) {

      val employeeCountByDepartmentAndGender = df
        .groupBy("Department", "Gender")
        .agg(count("*").as("Count"))
        .orderBy("Department", "Gender")

      employeeCountByDepartmentAndGender.show()

    } else {
      println("Header or trailer validation failed. Data processing aborted.")
    }

    spark.stop()
  }
}
