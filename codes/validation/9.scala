import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object my_object {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("my_object")
      .master("local[*]")
      .getOrCreate()

    val data = Seq(
      ("Karthik", 12000),
      ("Pratik", 15000),
      ("Mohan", 8000),
      ("Veer", 20000),
      ("Veena", 5000),
      ("Mohana", 18000)
    )

    val df = spark.createDataFrame(data).toDF("SalesPerson", "Sales")

    
    val header = df.first()
    val trailer = df.collect().last
    val expectedRecordCount = 6

    if (header.getString(0) == "SALES" && header.getString(1) == "v2" && header.getString(2).toInt == expectedRecordCount &&
      trailer.getString(0) == "TRAILER" && trailer.getString(1).toInt == expectedRecordCount &&
      df.count() - 2 == expectedRecordCount) {

      val salesStats = df
        .groupBy("SalesPerson")
        .agg(
          min("Sales").as("MinSales"),
          max("Sales").as("MaxSales")
        )

      salesStats.show()

    } else {
      println("Header or trailer validation failed. Data processing aborted.")
    }

    spark.stop()
  }
}
