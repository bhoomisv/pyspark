

from pyspark.sql.functions import col, lag, datediff
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("my code practice").getOrCreate()
data = [
    (1, "KitKat", 1000.0, "2021-01-01"),
    (1, "KitKat", 2000.0, "2021-01-02"),
    (1, "KitKat", 1000.0, "2021-01-03"),
    (1, "KitKat", 2000.0, "2021-01-04"),
    (1, "KitKat", 3000.0, "2021-01-05"),
    (1, "KitKat", 1000.0, "2021-01-06")
]
df = spark.createDataFrame(data, ["IT_ID", "IT_Name", "Price", "PriceDate"])
df.createOrReplaceTempView("my_table")

result_df = spark.sql("""
WITH PriceDiff AS (
  SELECT
    *,
    LAG(Price) OVER (PARTITION BY IT_ID ORDER BY PriceDate) AS PrevPrice,
    LAG(PriceDate) OVER (PARTITION BY IT_ID ORDER BY PriceDate) AS PrevDate,
    Price - LAG(Price) OVER (PARTITION BY IT_ID ORDER BY PriceDate) AS PriceDiff,
    DATEDIFF(PriceDate, LAG(PriceDate) OVER (PARTITION BY IT_ID ORDER BY PriceDate)) AS DaysDiff
  FROM
    my_table
)

SELECT
  IT_ID,
  IT_Name,
  PriceDate,
  Price,
  PrevPrice,
  PriceDiff,
  DaysDiff,
  PriceDiff / DaysDiff AS PriceDiffPerDay
FROM
  PriceDiff;
""")

result_df.show()

