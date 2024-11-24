import org.apache.spark.sql.{DataFrame, SparkSession}

object my_object {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("my_object")
      .master("local[*]")
      .getOrCreate()
    val data = Seq(
      ("Karthik", "123 Main St"),
      ("Pratik", null),
      ("Mohan", "456 Elm St"),
      ("Veer", null),
      ("Veena", "789 Oak St")
    )

    val df = spark.createDataFrame(data).toDF("Name", "Address")

    val header = df.first()
    val trailer = df.collect().last
    val expectedRecordCount = 5 

    if (header.getString(0) == "CUSTOMER_INFO" && header.getString(1) == "v1" && header.getString(2).toInt == expectedRecordCount &&
      trailer.getString(0) == "TRAILER" && trailer.getString(1).toInt == expectedRecordCount &&
      df.count() - 2 == expectedRecordCount) {

      val customersWithIncompleteAddresses = df.filter($"Address".isNull)

      customersWithIncompleteAddresses.show()

    } else {
      println("Header or trailer validation failed. Data processing aborted.")
    }

    spark.stop()
  }
}
