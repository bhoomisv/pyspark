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
df1= df.withColumn("PrevSalary", lag("salary", 1).over(windowSpec)).withColumn("SalaryDiff", col("salary") - col("PrevSalary"))
df1.show()

filtered_df = df1.filter(col("SalaryDiff") > 0)
filtered_df.show()

filtered_df1 = filtered_df.withColumn("lead_salary", lead("salary", 1).over(windowSpec)) \
                       .withColumn("lag_salary", lag("salary", 1).over(windowSpec))

filtered_df1.show()
