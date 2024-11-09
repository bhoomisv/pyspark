from pyspark.sql.functions import lag, lead, col, sum
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

windowSpec = Window.partitionBy().orderBy("id").rowsBetween(-2, 0)
df1 = df.withColumn("rolling_sum", sum("salary").over(windowSpec))

df1.show()
