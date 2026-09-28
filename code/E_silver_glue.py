import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import sum as _sum, count as _count

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SILVER = "s3://silver-ecommerce-sriram-2026-165811308743-ap-southeast-2-an/"
GOLD = "s3://gold-ecommerce-sriram-2026-165811308743-ap-southeast-2-an/"

# 1. Read from Silver layer - Cleaned datasets
orders_df = spark.read.parquet(SILVER + "orders/")
products_df = spark.read.parquet(SILVER + "products/")

print(f"Silver Orders: {orders_df.count()}, Products: {products_df.count()}")
orders_df.show(3)
products_df.show(3)

# 2. Join orders with products based on product_id
# Example: Order O001 contains product P013 - Join reveals product details (T-Shirt, Laptop etc.)
joined_df = orders_df.join(products_df, on="product_id", how="left")
joined_df.show(5)

# 3. GOLD REPORT 1: City-wise sales aggregation
city_sales = joined_df.groupBy("city").agg(
    _sum("amount").alias("total_sales"),
    _count("order_id").alias("total_orders")
)
city_sales.show()
city_sales.write.mode("overwrite").parquet(GOLD + "city_sales/")

# 4. GOLD REPORT 2: Product-wise sales performance
product_sales = joined_df.groupBy("product_name", "category").agg(
    _sum("amount").alias("total_sales"),
    _count("order_id").alias("times_sold")
)
product_sales.show()
product_sales.write.mode("overwrite").parquet(GOLD + "product_sales/")

print("Gold layer processing completed! Reports are ready!")
job.commit()
