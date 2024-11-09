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
patients_df = patients_df.withColumn("risk_level", \
    when((col("readmission_interval") < 15) & (col("age") > 60), "High Risk") \
    .when((col("readmission_interval") >= 15) & (col("readmission_interval") <= 30), "Moderate Risk") \
    .otherwise("Low Risk"))
patients_df.show()
risk_level_counts = patients_df.groupBy("risk_level").count()
risk_level_counts.show()

avg_high_risk_interval = patients_df.filter(col("risk_level") == "High Risk").agg(avg("readmission_interval").alias("avg_interval"))
avg_high_risk_interval.show()

moderate_risk_icu_patients = patients_df.filter((col("risk_level") == "Moderate Risk") & (col("icu_admissions") > 2))
moderate_risk_icu_patients.show()

