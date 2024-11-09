from pyspark.sql.functions import col, lag, lead
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
df.createOrReplaceTempView("my_table")

result_df = spark.sql("""
WITH SalaryChange AS (
  SELECT
    *,
    LAG(salary) OVER (ORDER BY id) AS PrevSalary,
    (salary - LAG(salary) OVER (ORDER BY id)) / LAG(salary) OVER (ORDER BY id) * 100 AS SalaryChangePercent
  FROM
    my_table
)

SELECT * FROM SalaryChange;
""")

result_df.show()
