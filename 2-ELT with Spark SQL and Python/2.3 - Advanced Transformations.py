# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets.py

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog hive_metastore;

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, profile:first_name, profile:address:country
# MAGIC from customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select from_json(profile) as profile_struct
# MAGIC from customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select profile 
# MAGIC from customers limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view parsed_customers as
# MAGIC    select customer_id, from_json(profile,schema_of_json('{"first_name":"Dniren","last_name":"Abby","gender":"Female","address":{"street":"768 Mesta Terrace","city":"Annecy","country":"France"}}')) as profile_struct
# MAGIC    from customers;
# MAGIC
# MAGIC select * from parsed_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE parsed_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, profile_struct.first_name, profile_struct.last_name from parsed_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view customers_final as
# MAGIC   select customer_id, profile_struct.*
# MAGIC   from parsed_customers;
# MAGIC
# MAGIC select * from customers_final;

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_id, customer_id, books from orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_id, customer_id, explode(books) as book
# MAGIC from orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, 
# MAGIC    collect_set(order_id) as orders_set,
# MAGIC    collect_set(books.book_id) as books_set
# MAGIC from orders
# MAGIC group by customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id,
# MAGIC   collect_set(books.book_id) as before_flatten,
# MAGIC   array_distinct(flatten(collect_set(books.book_id))) as after_flatten
# MAGIC   from orders
# MAGIC   group by customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view orders_enriched as
# MAGIC select *
# MAGIC from (
# MAGIC select *, explode(books) as book
# MAGIC from orders )O
# MAGIC inner join books b 
# MAGIC on o.book.book_id = b.book_Id;
# MAGIC
# MAGIC select * from orders_enriched;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE transactions AS
# MAGIC
# MAGIC SELECT * FROM (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     book.book_id AS book_id,
# MAGIC     book.quantity AS quantity
# MAGIC   FROM orders_enriched
# MAGIC ) PIVOT (
# MAGIC   sum(quantity) FOR book_id in (
# MAGIC     'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
# MAGIC     'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
# MAGIC   )
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM transactions
