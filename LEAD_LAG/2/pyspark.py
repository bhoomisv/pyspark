from pyspark.sql.functions import col, lag
from pyspark.sql import Window
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("my code practice").getOrCreate()

data = [
    (1, "John", 1000, "2016-01-01"),
    (1, "John", 2000, "2016-02-01"),
    (1, "John", 1000, "2016-03-01"),
    (1, "John", 2000, "2016-04-01"),
    (1, "John", 3000, "2016-05-01"),
    (1, "John", 1000, "2016-06-01")
]
df = spark.createDataFrame(data, ["ID", "NAME", "SALARY", "DATE"])

df1 = df.withColumn("PrevSalary", lag("SALARY").over(Window.partitionBy("ID").orderBy("DATE")))  \
    .withColumn("SalaryChange", when(col("SALARY") > col("PrevSalary"), "UP") \
                                   .when(col("SALARY") < col("PrevSalary"), "DOWN") \
                                   .otherwise("SAME"))  \
    .select("ID", "NAME", "DATE", "SALARY", "PrevSalary", "SalaryChange")

df1.show()
