from pyspark.sql.functions import lag, lead, col, sum,min
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

filtered_df = df.filter(col("salary") > 1500)
filtered_df.show()

windowSpec = Window.partitionBy().orderBy("id")
filtered_df = filtered_df.withColumn("lead_salary", lead("salary", 1).over(windowSpec)) \
                       .withColumn("lag_salary", lag("salary", 1).over(windowSpec))

filtered_df.show()
