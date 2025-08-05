# Databricks notebook source
# MAGIC %md
# MAGIC ## Using the catalog created for this use case

# COMMAND ----------

# MAGIC %run /Workspace/Users/mahatoruchita712@gmail.com/PySpark-use-cases/use_case1/utils
# MAGIC

# COMMAND ----------


"""
df_bronze = spark.read.format("delta") \
                      .option("multiline", "True") \
                      .table("customer_catalog.bronze.orders")
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading table in bronze using read_file function

# COMMAND ----------

df_bronze = read_file("bronze", "orders", "json", "dropmalformed")

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing to bronze layer using write_file function

# COMMAND ----------

df_bronze = write_file(df_bronze, "bronze", "orders", "json", "overwrite")
