
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, when, datediff, sum


spark = SparkSession.builder.appName("Inventory Analysis").getOrCreate()

inventory = [
("ProductA", 120),
("ProductB", 95),
("ProductC", 45),
("ProductD", 200),
("ProductE", 75),
("ProductF", 30),
("ProductG", 85),
("ProductH", 100),
("ProductI", 60),
("ProductJ", 20)
]
inventory_df = spark.createDataFrame(inventory, ["product_name", "stock_quantity"])
inventory_df.createOrReplaceTempView("inventory_table")

# Execute the SQL query
result_df = spark.sql("""
    SELECT stock_status, SUM(stock_quantity) AS total_stock FROM (
        SELECT 
            product_name,
            stock_quantity,
            CASE 
                WHEN stock_quantity > 100 THEN 'Overstocked'
                WHEN stock_quantity >= 50 AND stock_quantity <= 100 THEN 'Normal'
                ELSE 'Low Stock'
            END AS stock_status
        FROM inventory_table
    )
    GROUP BY stock_status
""")
result_df.show()
