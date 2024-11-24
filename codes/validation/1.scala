import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object my_object {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("my_object")
      .master("local[*]")
      .getOrCreate()

    
    val data = Seq(
      ("Karthik", "IT", 70000),
      ("Pratik", "IT", 75000),
      ("Hari", "HR", 55000),
      ("Mohan", "IT", 80000),
      ("Veer", "HR", 60000),
      ("Veena", "IT", 72000)
    )

    val df = spark.createDataFrame(data).toDF("Name", "Department", "Salary")

    
    val header = df.first()
    val trailer = df.collect().last
    val expectedRecordCount = 6

    if (header.getString(0) == "EMPLOYEE" && header.getString(1) == "v1" && header.getString(2).toInt == expectedRecordCount &&
      trailer.getString(0) == "TRAILER" && trailer.getString(1).toInt == expectedRecordCount &&
      df.count() - 2 == expectedRecordCount) {

      val averageSalaryByDepartment = df
        .groupBy("Department")
        .agg(avg("Salary").as("AverageSalary"))
        .orderBy("Department")

      averageSalaryByDepartment.show()

    } else {
      println("Header or trailer validation failed. Data processing aborted.")
    }

    spark.stop()
  }
}
