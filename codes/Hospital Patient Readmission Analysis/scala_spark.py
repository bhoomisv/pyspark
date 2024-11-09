import org.apache.spark.sql.functions._
val patients = List(
("Patient1", 62, 10, 3, "ICU"),
("Patient2", 45, 25, 1, "General"),
("Patient3", 70, 8, 2, "ICU"),
("Patient4", 55, 18, 3, "ICU"),
("Patient5", 65, 30, 1, "General"),
("Patient6", 80, 12, 4, "ICU"),
("Patient7", 50, 40, 1, "General"),
("Patient8", 78, 15, 2, "ICU"),
("Patient9", 40, 35, 1, "General"),
("Patient10", 73, 14, 3, "ICU")
).toDF("patient_id", "age", "readmission_interval", "icu_admissions", "admission_type")
val patientsWithRisk = patients.withColumn("risk_level", \
  when((col("readmission_interval") < 15) && (col("age") > 60), lit("High Risk")) \
    .when((col("readmission_interval") >= 15) && (col("readmission_interval") <= 30), lit("Moderate Risk")) \
    .otherwise(lit("Low Risk")))
patientsWithRisk.show()

val riskLevelCounts = patientsWithRisk.groupBy("risk_level").count()
riskLevelCounts.show()

val avgHighRiskInterval = patientsWithRisk.filter(col("risk_level") === "High Risk").agg(avg("readmission_interval").alias("avg_interval"))
avgHighRiskInterval.show()

val moderateRiskIcuPatients = patientsWithRisk.filter((col("risk_level") === "Moderate Risk") && (col("icu_admissions") > 2))
moderateRiskIcuPatients.show()
