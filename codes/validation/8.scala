import org.apache.spark.sql.{DataFrame, SparkSession}

object my_object {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("my_object")
      .master("local[*]")
      .getOrCreate()

    
    val data = Seq(
      ("Karthik", 82),
      ("Pratik", 78),
      ("Mohan", 90),
      ("Veer", 85),
      ("Mohana", 76)
    )

    val df = spark.createDataFrame(data).toDF("Name", "Score")

    
    val header = df.first()
    val trailer = df.collect().last
    val expectedRecordCount = 5

    if (header.getString(0) == "STUDENTS" && header.getString(1) == "v1" && header.getString(2).toInt == expectedRecordCount &&
      trailer.getString(0) == "TRAILER" && trailer.getString(1).toInt == expectedRecordCount &&
      df.count() - 2 == expectedRecordCount) {

      val topScorers = df.filter($"Score" > 80)

      topScorers.show()

    } else {
      println("Header or trailer validation failed. Data processing aborted.")
    }

    spark.stop()
  }
}
