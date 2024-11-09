from pyspark.sql.functions import col, lag, lead,sum
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
WITH RollingSum AS (
  SELECT
    *,
    SUM(salary) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_sum
  FROM
    my_table
)

SELECT * FROM RollingSum;
""")

result_df.show()