# Databricks notebook source
# MAGIC %run /Workspace/Users/mahatoruchita712@gmail.com/PySpark-use-cases/use_case1/utils

# COMMAND ----------

df_bronze = read_file("bronze", "orders", "json")

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

df = flatten_column(df_bronze)

# COMMAND ----------

df.display()

# COMMAND ----------

display(df_bronze.select("customer").dtypes)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("Customer_id", col("customer.id")) \
                     .withColumn("Name", col("customer.name")) \
                     .withColumn("Email", col("customer.email"))

df_silver = df_silver.drop("customer")

display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("City", col("shipping_address.city")) \
                     .withColumn("State", col("shipping_address.state")) \
                     .withColumn("Country", col("shipping_address.country"))

df_silver = df_silver.drop("shipping_address")

df_silver.display()

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------



# COMMAND ----------

df_silver = df_silver.withColumn("Product_id", col("item.product_id")) \
                     .withColumn("Product_name", col("item.product_name")) \
                     .withColumn("Quantity", col("item.quantity")) \
                     .withColumn("Price", col("item.price").cast(DoubleType()))
                         
df_silver = df_silver.drop("item")

display(df_silver)

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver = df_silver.withColumn("Total_amount", col("Price") * col("Quantity"))

# COMMAND ----------

df_silver = df_silver.fillna("NaN", subset=["Price", "Total_amount"])

# COMMAND ----------

df_silver.display()

# COMMAND ----------

df_silver = df_silver.withColumn("Order_date", try_to_timestamp("order_date"))
df_silver.printSchema()

# COMMAND ----------

df_silver.display()

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

df_silver = df_silver.withColumn("Status", col("status"))

# COMMAND ----------

df_silver = df_silver.select(
    col("Order_id"), 
    col("Order_date"), 
    col("Customer_id"),
    col("Name"),
    col("Email"),
    col("Product_id"),
    col("Product_name"),
    col("Quantity"),
    col("Price"),
    col("Total_amount"),
    col("City"),
    col("State"),
    col("Country"),
    col("Status")
    )

df_silver.display()

# COMMAND ----------

df_silver = write_file(df_silver, "silver", "append")
