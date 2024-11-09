from pyspark.sql.functions import lag, lead
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

windowSpec = Window.partitionBy().orderBy("id")
df1 = df.withColumn("lead_salary", lead("salary", 1).over(windowSpec)) \
        .withColumn("lag_salary", lag("salary", 1).over(windowSpec))
df1.show()
