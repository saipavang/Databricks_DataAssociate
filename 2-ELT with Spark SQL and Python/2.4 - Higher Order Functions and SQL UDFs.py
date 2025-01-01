# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets.py

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Higher Order Function examples

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   order_id,
# MAGIC   books,
# MAGIC   filter(books, i -> i.quantity >= 2) as multiple_copies
# MAGIC   from orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_id, multiple_copies
# MAGIC from (
# MAGIC   select 
# MAGIC   order_id,
# MAGIC   filter(books,i -> i.quantity >= 2) as multiple_copies from orders)
# MAGIC   where size(multiple_copies) >0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC order_id, books,
# MAGIC  TRANSFORM (
# MAGIC   books, b -> CAST(b.subtotal * 0.8 AS INT)
# MAGIC  ) as subtotal_after_discount
# MAGIC from orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function get_url(email string)
# MAGIC returns string
# MAGIC
# MAGIC return concat("https://www.",split(email,"@")[1])

# COMMAND ----------

# MAGIC %sql
# MAGIC select email, get_url(email) as domain
# MAGIC from customers;

# COMMAND ----------

# MAGIC %sql describe function get_url

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION site_type(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC           WHEN email like "%.com" THEN "Commercial business"
# MAGIC           WHEN email like "%.org" THEN "Non-profits organization"
# MAGIC           WHEN email like "%.edu" THEN "Educational institution"
# MAGIC           ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
# MAGIC        END;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, site_type(email) as domain_category
# MAGIC FROM customers
