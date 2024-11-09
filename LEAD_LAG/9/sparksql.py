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
WITH SalaryDiff AS (
  SELECT
    *,
    LAG(salary) OVER (PARTITION BY name ORDER BY id) AS PrevSalary,
    ABS(salary - LAG(salary) OVER (PARTITION BY name ORDER BY id)) AS SalaryDiff
  FROM
    my_table
),
FilteredData AS (
  SELECT *
  FROM SalaryDiff
  WHERE SalaryDiff > 500
)

SELECT
  *,
  LEAD(salary) OVER (PARTITION BY name ORDER BY id) AS lead_salary,
  LAG(salary) OVER (PARTITION BY name ORDER BY id) AS lag_salary
FROM
  FilteredData;
""")

result_df.show()
