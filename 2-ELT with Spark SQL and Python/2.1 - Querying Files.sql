-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying JSON

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets.py

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo-datasets/bookstore'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

select * from json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

select * from json.`${dataset.bookstore}/customers-json/export_*.json`

-- COMMAND ----------

select * from json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

select count(*) from json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

select *, input_file_name() as source_file from json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

select * from text.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

select * from binaryFile.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

select * from csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

use catalog hive_metastore;
create table books_csv
   (book_Id string,title string, author string, category string, price double)
  using csv
  OPTIONS (
    header = "true",
    delimiter = ";"
  )
  location "${dataset.bookstore}/books-csv"

-- COMMAND ----------

select * from books_csv;

-- COMMAND ----------

describe extended books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read.table("books_csv")
-- MAGIC  .write.mode("append")
-- MAGIC  .format("csv").option("header", "true").option('delimiter', ';').save(f"{dataset_bookstore}/books-csv")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

select count(*) from books_csv

-- COMMAND ----------

refresh table books_csv

-- COMMAND ----------

select count(*) from books_csv

-- COMMAND ----------

create table customers1 as
select * from json.`${dataset.bookstore}/customers-json`;

describe extended customers;

-- COMMAND ----------

use catalog hive_metastore;

-- COMMAND ----------

create table books_unparsed as
select * from csv.`${dataset.bookstore}/books-csv`;

-- COMMAND ----------

select * from books_unparsed;

-- COMMAND ----------

create temp view books_tmp_vw 
       (book_id string,title string,author string,category string,price double)
using csv
 options (
    path = "${dataset.bookstore}/books-csv/export_*.csv",
    header = "true",
    delimiter = ';'
 );

 create table books as
    select * from books_tmp_vw;

  select * from books;
