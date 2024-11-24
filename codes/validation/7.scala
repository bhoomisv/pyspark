import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object my_object {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("my_object")
      .master("local[*]")
      .getOrCreate()

    
    val data = Seq(
      ("Karthik", 40),
      ("Pratik", 35),
      ("Mohan", 45),
      ("Veer", 30),
      ("Veena", 50),
      ("Mohana", 40)
    )

    val df = spark.createDataFrame(data).toDF("Name", "Hours")

    
    val header = df.first()
    val trailer = df.collect().last
    val expectedRecordCount = 6

    if (header.getString(0) == "TIMESHEET" && header.getString(1) == "v1" && header.getString(2).toInt == expectedRecordCount &&
      trailer.getString(0) == "TRAILER" && trailer.getString(1).toInt == expectedRecordCount &&
      df.count() - 2 == expectedRecordCount) {

      val totalHoursByEmployee = df
        .groupBy("Name")
        .agg(sum("Hours").as("TotalHours"))

      totalHoursByEmployee.show()

    } else {
      println("Header or trailer validation failed. Data processing aborted.")
    }

    spark.stop()
  }
}
