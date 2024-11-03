from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum
spark = SparkSession.builder.appName("Product Return Rate Analysis").getOrCreate()
products = [
    ("Laptop", "Electronics", 120, 45),
    ("Smartphone", "Electronics", 80, 60),
    ("Tablet", "Electronics", 50, 72),
    ("Headphones", "Accessories", 110, 47),
    ("Shoes", "Clothing", 90, 55),
    ("Jacket", "Clothing", 30, 80),
    ("TV", "Electronics", 150, 40),
    ("Watch", "Accessories", 60, 65),
    ("Pants", "Clothing", 25, 75),
    ("Camera", "Electronics", 95, 58)
]

products_df = spark.createDataFrame(products, ["product_name", "category", "return_count", "satisfaction_score"])
