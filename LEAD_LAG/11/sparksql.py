from pyspark.sql.functions import col, lag, lead,sum, min, abs, when
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
WITH RunningTotal AS (
  SELECT
    *,
    SUM(salary) OVER (PARTITION BY name ORDER BY id) AS running_total
  FROM
    my_table
)

SELECT * FROM RunningTotal;
""")

result_df.show()