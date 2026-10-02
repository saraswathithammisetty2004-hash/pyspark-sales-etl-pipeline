# Databricks notebook source
data = [
    (1001, "2026-01-05", "C001", "P001", "Laptop", "Electronics", 2, 50000, "South"),
    (1002, "2026-01-06", "C002", "P002", "Mouse", "Electronics", 5, 800, "North"),
    (1003, "2026-01-07", "C003", "P003", "Chair", "Furniture", 1, 7000, "West"),
    (1004, "2026-01-08", "C001", "P004", "Desk", "Furniture", 2, 12000, "South"),
    (1005, "2026-01-09", "C004", "P005", "Keyboard", "Electronics", 3, 1500, "East")
]

columns = [
    "order_id",
    "order_date",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "region"
]

# COMMAND ----------

df=spark.createDataFrame(data,columns)
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("bronze_sales")

# COMMAND ----------

bronze_df = spark.table("bronze_sales")

bronze_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_sales;

# COMMAND ----------

bronze_df = spark.table("bronze_sales")

bronze_df.show()

# COMMAND ----------

bronze_df.count()
bronze_df.dropDuplicates().count()

# COMMAND ----------

from pyspark.sql.functions import col, to_date

silver_df = bronze_df.withColumn(
    "order_date",
    to_date(col("order_date"))
)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

silver_df = silver_df.dropDuplicates()
silver_df = silver_df.dropna(
    subset=[
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price"
    ]
)

# COMMAND ----------

silver_df = silver_df.withColumn(
    "total_amount",
    col("quantity") * col("unit_price")
)

# COMMAND ----------

silver_df.show()

# COMMAND ----------

silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_sales;

# COMMAND ----------

silver_df=spark.table("silver_sales")
silver_df.show()

# COMMAND ----------

from pyspark.sql.functions import sum, count

gold_category = silver_df.groupBy("category").agg(
    sum("total_amount").alias("total_sales"),
    sum("quantity").alias("total_quantity"),
    count("order_id").alias("number_of_orders")
)

# COMMAND ----------

gold_category.show()

# COMMAND ----------

gold_category.write.format("delta").mode("overwrite").saveAsTable(
    "gold_sales_by_category"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_sales_by_category;

# COMMAND ----------

gold_region = silver_df.groupBy("region").agg(
    sum("total_amount").alias("total_sales"),
    sum("quantity").alias("total_quantity"),
    count("order_id").alias("number_of_orders")
)



# COMMAND ----------

gold_region.write.format("delta").mode("overwrite").saveAsTable(
    "gold_sales_by_region"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_sales_by_region;

# COMMAND ----------

gold_product = silver_df.groupBy(
    "product_id",
    "product_name"
).agg(
    sum("total_amount").alias("total_sales"),
    sum("quantity").alias("total_quantity"),
    count("order_id").alias("number_of_orders")
)

# COMMAND ----------

gold_product.write.format("delta").mode("overwrite").saveAsTable(
    "gold_sales_by_product"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_sales_by_product

# COMMAND ----------

