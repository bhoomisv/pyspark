

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
WITH SalaryLeadLag AS (
  SELECT
    *,
    LEAD(salary) OVER (ORDER BY id) AS lead_salary,
    LAG(salary) OVER (ORDER BY id) AS lag_salary
  FROM
    my_table
)

SELECT * FROM SalaryLeadLag;
""")

result_df.show()
