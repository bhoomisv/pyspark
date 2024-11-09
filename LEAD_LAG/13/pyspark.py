from pyspark.sql.functions import lag, lead, col, sum,min,max, abs, when, lit,avg
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

windowSpec = Window.partitionBy("name")
df1 = df.withColumn("avg_salary", avg("salary").over(windowSpec)) \
    .withColumn("salary_diff", col("salary") - col("avg_salary"))


df1.show()
