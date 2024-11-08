from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum, when, col, max, avg
spark = SparkSession.builder.appName("my code practice").getOrCreate()
electricity_usage = [
("House1", 550, 250),
("House2", 400, 180),
("House3", 150, 50),
("House4", 500, 200),
("House5", 600, 220),
("House6", 350, 120),
("House7", 100, 30),
("House8", 480, 190),
("House9", 220, 105),
("House10", 150, 60)
]
electricity_usage_df = spark.createDataFrame(electricity_usage, ["household", "kwh_usage", "total_bill"])
electricity_usage_df.createOrReplaceTempView("electricity_usage_table")

result_df = spark.sql("""
SELECT
  household,
  kwh_usage,
  total_bill,
  CASE
    WHEN kwh_usage BETWEEN 200 AND 500 AND total_bill BETWEEN 100 AND 200 THEN 'Medium Usage'
    ELSE 'Low Usage'
  END AS usage_category
FROM
  electricity_usage_table;
""")
result_df.show()
result_df1 = spark.sql("""
SELECT
  usage_category,
  COUNT(*) AS household_count
FROM
  (
    SELECT
      household,
      kwh_usage,
      total_bill,
      CASE
        WHEN kwh_usage BETWEEN 200 AND 500 AND total_bill BETWEEN 100 AND 200 THEN 'Medium Usage'
        ELSE 'Low Usage'
      END AS usage_category
    FROM
      electricity_usage_table
  )
GROUP BY
  usage_category;
""")
result_df1.show()
result_df2 = spark.sql("""
SELECT
  MAX(total_bill) AS max_high_usage_bill
FROM
  electricity_usage_table
WHERE
  kwh_usage > 500;
""")
result_df2.show()
result_d3f = spark.sql("""
SELECT
  AVG(kwh_usage) AS avg_medium_usage_kwh
FROM
  (
    SELECT
      household,
      kwh_usage,
      total_bill,
      CASE
        WHEN kwh_usage BETWEEN 200 AND 500 AND total_bill BETWEEN 100 AND 200 THEN 'Medium Usage'
        ELSE 'Low Usage'
      END AS usage_category
    FROM
      electricity_usage_table
  )
WHERE
  usage_category = 'Medium Usage';
""")
result_df3.show()

result_df44 = spark.sql("""
SELECT
  COUNT(*) AS low_usage_high_kwh_count
FROM
  (
    SELECT
      household,
      kwh_usage,
      total_bill,
      CASE
        WHEN kwh_usage BETWEEN 200 AND 500 AND total_bill BETWEEN 100 AND 200 THEN 'Medium Usage'
        ELSE 'Low Usage'
      END AS usage_category
    FROM
      electricity_usage_table
  )
WHERE
  usage_category = 'Low Usage' AND kwh_usage > 300;
""")
result_df4.show()
