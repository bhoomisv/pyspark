import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object my_object {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("my_object")
      .master("local[*]")
      .getOrCreate()

    
    val data = Seq(
      ("North", "Karthik", 10000),
      ("South", "Pratik", 15000),
      ("East", "Hari", 8000),
      ("West", "Mohan", 12000),
      ("North", "Veer", 7000)
    )

    val df = spark.createDataFrame(data).toDF("Region", "SalesPerson", "Sales")

    
    val header = df.first()
    val trailer = df.collect().last
    val expectedRecordCount = 5

    if (header.getString(0) == "SALES" && header.getString(1) == "v2" && header.getString(2).toInt == expectedRecordCount &&
      trailer.getString(0) == "TRAILER" && trailer.getString(1).toInt == expectedRecordCount &&
      df.count() - 2 == expectedRecordCount) {

      val totalSalesByRegion = df
        .groupBy("Region")
        .agg(sum("Sales").as("TotalSales"))
        .orderBy("Region")

      totalSalesByRegion.show()

    } else {
      println("Header or trailer validation failed. Data processing aborted.")
    }

    spark.stop()
  }
}
