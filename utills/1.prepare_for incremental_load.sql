-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/myformula1projectdl/processed"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_performance CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_performance
LOCATION "/mnt/myformula1projectdl/performance"

-- COMMAND ----------


