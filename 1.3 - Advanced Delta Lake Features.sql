-- Databricks notebook source
use catalog hive_metastore

-- COMMAND ----------

describe history employees

-- COMMAND ----------

select * 
from employees version as of 1

-- COMMAND ----------

delete from employees

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

restore table employees to version as of 6

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

optimize employees
zorder by id

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

VAcuUM employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

vacuum employees retain 0 hours

-- COMMAND ----------

set spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

vacuum employees retain 0 hours

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

select * from employees@v1

-- COMMAND ----------

select * from employees
