# Databricks notebook source
# MAGIC %run /Workspace/Users/mahatoruchita712@gmail.com/PySpark-use-cases/use_case1/utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading the file from bronze

# COMMAND ----------

df_bronze = read_file("bronze", "orders", "json", "permissive")

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flattening the nested struct and array column

# COMMAND ----------

df_silver = flatten_column(df_bronze)

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# display(df_bronze.select("customer").dtypes)

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

"""
df_silver = df_silver.withColumn("Customer_id", col("customer.id")) \
                     .withColumn("Name", col("customer.name")) \
                     .withColumn("Email", col("customer.email"))

df_silver = df_silver.drop("customer")

display(df_silver)
"""

# COMMAND ----------

"""
df_silver = df_silver.withColumn("City", col("shipping_address.city")) \
                     .withColumn("State", col("shipping_address.state")) \
                     .withColumn("Country", col("shipping_address.country"))

df_silver = df_silver.drop("shipping_address")

df_silver.display()
"""

# COMMAND ----------

#df_silver.printSchema()

# COMMAND ----------

"""
df_silver = df_silver.withColumn("Product_id", col("item.product_id")) \
                     .withColumn("Product_name", col("item.product_name")) \
                     .withColumn("Quantity", col("item.quantity")) \
                     .withColumn("Price", col("item.price").cast(DoubleType()))
                         
df_silver = df_silver.drop("item")

display(df_silver)
"""

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Casting the price column from string to Double to handle decimal values.

# COMMAND ----------

df_silver = df_silver.withColumn("items_price", col("items_price").cast(DoubleType()))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a column Total_amount which is the product of items_price and items quantity.

# COMMAND ----------

df_silver = df_silver.withColumn("Total_amount", col("items_price") * col("items_quantity"))

# COMMAND ----------

#df_silver = df_silver.fillna("NaN", subset=["Price", "Total_amount"])

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Converting the order_date column from string to date type : 
# MAGIC - First converting it to timestamp then to date type

# COMMAND ----------

df_silver = df_silver.withColumn("Order_date", try_to_timestamp("order_date"))
df_silver.printSchema()

# COMMAND ----------


df_silver = df_silver.withColumn("Order_date", \
                                when(to_date(col("order_date"), "yyyy-MM-dd").isNotNull(), \
                                     to_date(col("order_date"), 'yyyy-MM-dd')) \
                                     .otherwise(None)
                                     )


# COMMAND ----------

df_silver.printSchema()
df_silver.display()

# COMMAND ----------

""" df_silver = df_silver.select(
    col("order_id"), 
    col("Order_date"), 
    col("customer_id"),
    col("customer_name"),
    col("customer_email"),
    col("items_product_id"),
    col("items_product_name"),
    col("items_quantity"),
    col("items_price"),
    col("Total_amount"),
    col("shipping_address_city"),
    col("shipping_address_state"),
    col("shipping_address_country"),
    col("status")
    )

df_silver.display()
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## writing to silver layer using write file function

# COMMAND ----------

df_silver = write_file(df_silver, "silver", "orders", "ignore")
