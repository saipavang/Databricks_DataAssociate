# Databricks notebook source
files = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore")
display(files)
