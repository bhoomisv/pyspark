from pyspark.sql.functions import lag, lead, col, sum,min,max, abs, when, lit,avg, row_number
from pyspark.sql import Window
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("my code practice").getOrCreate()

data = [
    (1, "karthik", 1000),
    (2, "moahn", 2000),
    (3, "vinay", 1500),
    (4, "Deva", 3000)
]
df = spark.createDataFrame(data, ["id", "name", "salary"])

windowSpec = Window.partitionBy("name").orderBy("id")
df1 = df.withColumn("PrevSalary", lag("salary", 1).over(windowSpec)).withColumn("SalaryChangePercent", \
							when(col("PrevSalary").isNotNull(), (col("salary") - col("PrevSalary")) / col("PrevSalary") * 100) \
							.otherwise(None))
df1.show()
