-- Databricks notebook source
use catalog hive_metastore;

-- COMMAND ----------

create table employees
-- using delta
  (id INT,name string, salary double);

-- COMMAND ----------

INSERT INTO employees
VALUES 
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5);

INSERT INTO employees
VALUES
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3);

INSERT INTO employees
VALUES
  (5, "Anna", 2500.0);

INSERT INTO employees
VALUES
  (6, "Kim", 6200.3)

-- COMMAND ----------

select * from employees;

-- COMMAND ----------

describe detail employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

update employees set salary = salary + 100
where name like "A%"

-- COMMAND ----------

select * from employees;

-- COMMAND ----------

describe detail employees;

-- COMMAND ----------

describe history employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log'
