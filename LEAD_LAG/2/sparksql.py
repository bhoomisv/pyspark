



from pyspark.sql.functions import col, lag
from pyspark.sql import Window
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("my code practice").getOrCreate()

data = [
    (1, "John", 1000, "2016-01-01"),
    (1, "John", 2000, "2016-02-01"),
    (1, "John", 1000, "2016-03-01"),
    (1, "John", 2000, "2016-04-01"),
    (1, "John", 3000, "2016-05-01"),
    (1, "John", 1000, "2016-06-01")
]
df = spark.createDataFrame(data, ["ID", "NAME", "SALARY", "DATE"])
df.createOrReplaceTempView("my_table")

result_df = spark.sql("""
WITH SalaryDiff AS (
  SELECT
    *,
    LAG(SALARY) OVER (PARTITION BY ID ORDER BY DATE) AS PrevSalary,
    CASE
      WHEN SALARY > LAG(SALARY) OVER (PARTITION BY ID ORDER BY DATE) THEN 'UP'
      WHEN SALARY < LAG(SALARY) OVER (PARTITION BY ID ORDER BY DATE) THEN 'DOWN'
      ELSE 'SAME'
    END AS SalaryChange
  FROM
    my_table
)

SELECT * FROM SalaryDiff;
""")

result_df.show()
