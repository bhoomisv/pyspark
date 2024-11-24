import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object my_object {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("my_object")
      .master("local[*]")
      .getOrCreate()

    
    val data = Seq(
      ("Karthik", 30),
      ("Pratik", 45),
      ("Mohan", 20),
      ("Veer", 60),
      ("Veena", 25),
      ("Mohana", 33)
    )

    val df = spark.createDataFrame(data).toDF("Name", "Age")

    
    val header = df.first()
    val trailer = df.collect().last
    val expectedRecordCount = 6

    if (header.getString(0) == "CUSTOMER" && header.getString(1) == "v3" && header.getString(2).toInt == expectedRecordCount &&
      trailer.getString(0) == "TRAILER" && trailer.getString(1).toInt == expectedRecordCount &&
      df.count() - 2 == expectedRecordCount) {

      val ageGroupDF = df.withColumn("AgeGroup", when($"Age" >= 20 && $"Age" <= 30, "20-30")
        .when($"Age" >= 31 && $"Age" <= 40, "31-40")
        .when($"Age" >= 41 && $"Age" <= 50, "41-50")
        .otherwise("50+"))
        .groupBy("AgeGroup")
        .agg(count("*").as("Count"))

      ageGroupDF.show()

    } else {
      println("Header or trailer validation failed. Data processing aborted.")
    }

    spark.stop()
  }
}
