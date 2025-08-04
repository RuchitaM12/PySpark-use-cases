# Databricks notebook source
# MAGIC %sql
# MAGIC USE catalog customer_catalog;
# MAGIC

# COMMAND ----------


df_bronze = spark.read.format("delta") \
                      .option("multiline", "True") \
                      .table("customer_catalog.bronze.orders")

# COMMAND ----------

display(df_bronze)

# COMMAND ----------


