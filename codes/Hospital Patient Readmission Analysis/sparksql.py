

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum, when, col, max, avg, min, count, Window
spark = SparkSession.builder.appName("my code practice").getOrCreate()

patients = [
("Patient1", 62, 10, 3, "ICU"),
("Patient2", 45, 25, 1, "General"),
("Patient3", 70, 8, 2, "ICU"),
("Patient4", 55, 18, 3, "ICU"),
("Patient5", 65, 30, 1, "General"),
("Patient6", 80, 12, 4, "ICU"),
("Patient7", 50, 40, 1, "General"),
("Patient10", 73, 14, 3, "ICU")
]
patients_df = spark.createDataFrame(patients, ["patient_id", "age", "readmission_interval", "icu_admissions", "admission_type"])
patients_df.createOrReplaceTempView("patients_table")

result_df = spark.sql("""
SELECT *,
       CASE
           WHEN readmission_interval < 15 AND age > 60 THEN 'High Risk'
           WHEN readmission_interval BETWEEN 15 AND 30 THEN 'Moderate Risk'
           ELSE 'Low Risk'
       END AS risk_level
FROM patients_table;
""")
result_df.show()
result_df1 = spark.sql("""
SELECT risk_level, COUNT(*) AS patient_count
FROM (
  SELECT *,
         CASE
             WHEN readmission_interval < 15 AND age > 60 THEN 'High Risk'
             WHEN readmission_interval BETWEEN 15 AND 30 THEN 'Moderate Risk'
             ELSE 'Low Risk'
         END AS risk_level
  FROM patients_table
)
GROUP BY risk_level;
""")
result_df1.show()
result_df2 = spark.sql("""
SELECT AVG(readmission_interval) AS avg_interval
FROM patients_table
WHERE risk_level = 'High Risk';
""")
result_df2.show()
result_df3 = spark.sql("""
SELECT *
FROM patients_table
WHERE risk_level = 'Moderate Risk' AND icu_admissions > 2;
""")
result_df3.show()
