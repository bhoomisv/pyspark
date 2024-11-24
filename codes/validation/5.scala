import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object my_object {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("my_object")
      .master("local[*]")
      .getOrCreate()

    
    val data = Seq(
      ("Laptop", 10000),
      ("Mouse", 0),
      ("Keyboard", 5000),
      ("Monitor", 0),
      ("Printer", 7000)
    )

    val df = spark.createDataFrame(data).toDF("Product", "Sales")

    
    val header = df.first()
    val trailer = df.collect().last
    val expectedRecordCount = 5

    if (header.getString(0) == "PRODUCT_SALES" && header.getString(1) == "v1" && header.getString(2).toInt == expectedRecordCount &&
      trailer.getString(0) == "TRAILER" && trailer.getString(1).toInt == expectedRecordCount &&
      df.count() - 2 == expectedRecordCount) {

      val zeroSalesProducts = df.filter($"Sales" === 0)

      zeroSalesProducts.show()

    } else {
      println("Header or trailer validation failed. Data processing aborted.")
    }

    spark.stop()
  }

}
