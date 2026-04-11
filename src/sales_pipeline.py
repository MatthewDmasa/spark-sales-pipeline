from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.functions import sum as spark_sum, count, month, year, desc

"""
E-Commerce Sales Data Pipeline (PySpark)

This script:
1. Loads roaw CSV sales data
2. Cleans and validates records
3. Transforms data (Date + revenue)
4. Generates aggregated insights
5. Saves outputs (Pandas fallback for Windows compatibility)
"""

# Create Spark session
# Set Spark to run on all cores

spark = (
    SparkSession.builder
    .appName("ECommerceSalesPipeline")
    .master("local[*]")
    .config("spark.hadoop.hadoop.native.lib", "false")
    .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")
    .getOrCreate()
)

# Reduce log noise in terminal

spark.sparkContext.setLogLevel("Warn")

# Load CSV file into Spark Dataframe

df = (
	spark.read
	.option("header",True)
	.option("inferSchema",True)
	.csv("data/raw/sales_data.csv")
)

print("\n===RAW DATA===")
df.show(5)

print(f"Raw row count: {df.count()}")

#
# 1 - Remove Duplicates
#

df = df.dropDuplicates()

print("\nAfter removing duplicates:")
print(f"Row count: {df.count()}")

#
# 2 - Handle null values
#

critical_columns = [
	"order_id",
	"order_date",
	"customer_id",
	"product",
	"price",
	"quantity",
	"region"
]

df = df.dropna(subset=critical_columns)

print("\nAfter dropping nulls:")
print(f"Row count: {df.count()}")

#
# 3 - Convert date column
#

df = df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

df = df.dropna(subset=["order_date"])

#
# 4 - Filter bad values
#

df = df.filter((col("price") > 0) & (col("quantity") > 0))

#
# 5 - Add revenue column
#
df = df.withColumn("revenue", col("price") * col("quantity"))

print ("\n=== CLEANED DATA===")
df.show(5)

print(f"Cleaned row count: {df.count()}")



#
# INSIGHT 1
#

print("\n===TOP PRODUCTS BY REVENUE===")

top_products = (
    df.groupBy("product")
    .agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        spark_sum("revenue").alias("total_revenue")
    )
    .orderBy(desc("total_revenue"))
)

top_products.show()

print

#
# INSIGHT 2
#

print("\n===SALES BY REGION===")

sales_by_region = (
	df.groupBy("region")
	.agg(
		spark_sum("revenue").alias("total_revenue"),
		count("order_id").alias("total_orders")
	)
	.orderBy(desc("total_revenue"))
)

sales_by_region.show()

#
# INSIGHT 3
#

print("\n MONTHLY TRENDS")

df = df.withColumn("order_year", year("order_date"))
df = df.withColumn("order_month", month("order_date"))

monthly_trends = (
	df.groupBy("order_year", "order_month")
	.agg(
		spark_sum("revenue").alias("monthly_revenue"),
		count("order_id").alias("monthly_orders")
	)
	.orderBy("order_year","order_month")
)

monthly_trends.show()

print("Saving cleaned data (Pandas fallback)...")

pdf = df.toPandas()
pdf.to_csv("data/processed/clean_sales_data.csv", index=False)

print("Saving top products...")
top_products_pd = top_products.toPandas()
top_products_pd.to_csv("output/top_products.csv", index=False)

print("Saving sales by region output...")

sales_by_region_pd = sales_by_region.toPandas()
sales_by_region_pd.to_csv("output/sales_by_region.csv", index=False)

print("Saving monthly trends output...")

monthly_trends_pd = monthly_trends.toPandas()
monthly_trends_pd.to_csv("output/monthly_trends.csv", index=False)

print("\nAll outputs saved successfully.")

spark.stop()






