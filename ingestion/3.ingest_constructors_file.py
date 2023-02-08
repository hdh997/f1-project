# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest constuctors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ####Step1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/myformula1projectdl/raw/

# COMMAND ----------

constructor_df = spark.read.schema(constructors_schema).json("/mnt/myformula1projectdl/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Drop unwanted column

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(constructor_df.url)

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed('constructorId', 'constructor_id')\
.withColumnRenamed('constructorRef', 'constructor_ref')\
.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step5 - Write output to parque file

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet("/mnt/myformula1projectdl/processed/constructor")
