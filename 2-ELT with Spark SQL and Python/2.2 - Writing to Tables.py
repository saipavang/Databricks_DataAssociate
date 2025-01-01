# Databricks notebook source
# MAGIC
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets.py

# COMMAND ----------

# MAGIC %sql
# MAGIC create table orders as 
# MAGIC select * from parquet.`${dataset.bookstore}/orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table orders as
# MAGIC select * from parquet.`${dataset.bookstore}/orders`

# COMMAND ----------

# MAGIC %sql describe history orders

# COMMAND ----------

# MAGIC %sql
# MAGIC insert overwrite orders
# MAGIC select * from parquet.`${dataset.bookstore}/orders`

# COMMAND ----------

# MAGIC %sql describe history orders

# COMMAND ----------

# MAGIC %sql
# MAGIC insert overwrite orders
# MAGIC select *, current_timestamp() from parquet.`${dataset.bookstore}/orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into orders
# MAGIC select * from parquet.`${dataset.bookstore}/orders-new`

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders
